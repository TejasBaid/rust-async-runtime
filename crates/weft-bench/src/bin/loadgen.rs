//! A small closed-loop HTTP/1.1 load generator (wrk-style).
//!
//! Each connection runs on its own OS thread with blocking sockets, so the
//! client shares no code with the runtime under test. Every connection keeps
//! `--pipeline` requests in flight, waits for all responses, and records the
//! batch latency.
//!
//! ```text
//! cargo run --release -p weft-bench --bin loadgen -- \
//!     --addr 127.0.0.1:8080 --path /plaintext --connections 64 --duration 10 --pipeline 1
//! ```

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

struct Args {
    addr: SocketAddr,
    path: String,
    connections: usize,
    duration: Duration,
    warmup: Duration,
    pipeline: usize,
}

fn parse_args() -> Args {
    let mut args = Args {
        addr: "127.0.0.1:8080".parse().unwrap(),
        path: "/plaintext".into(),
        connections: 64,
        duration: Duration::from_secs(10),
        warmup: Duration::from_secs(1),
        pipeline: 1,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || {
            it.next()
                .unwrap_or_else(|| panic!("missing value for {flag}"))
        };
        match flag.as_str() {
            "--addr" => args.addr = value().parse().expect("--addr"),
            "--path" => args.path = value(),
            "--connections" | "-c" => args.connections = value().parse().expect("-c"),
            "--duration" | "-d" => {
                args.duration = Duration::from_secs_f64(value().parse().expect("-d"))
            }
            "--warmup" => args.warmup = Duration::from_secs_f64(value().parse().expect("--warmup")),
            "--pipeline" | "-p" => args.pipeline = value().parse::<usize>().expect("-p").max(1),
            "--help" | "-h" => {
                eprintln!(
                    "usage: loadgen [--addr A] [--path P] [-c CONNS] [-d SECS] [--warmup SECS] [-p DEPTH]"
                );
                std::process::exit(0);
            }
            other => panic!("unknown flag {other}"),
        }
    }
    args
}

/// Reads exactly one response from `buf`/`stream`, returning bytes consumed.
fn read_response(stream: &mut TcpStream, buf: &mut Vec<u8>, start: &mut usize) -> io::Result<()> {
    loop {
        let avail = &buf[*start..];
        if let Some(head_end) = avail.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = &avail[..head_end];
            let mut content_length = 0usize;
            for line in head.split(|&b| b == b'\n') {
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                if line.len() > 15 && line[..15].eq_ignore_ascii_case(b"content-length:") {
                    content_length = std::str::from_utf8(&line[15..])
                        .ok()
                        .and_then(|s| s.trim().parse().ok())
                        .ok_or_else(|| io::Error::other("bad content-length"))?;
                }
            }
            if !head.starts_with(b"HTTP/1.1 200") {
                return Err(io::Error::other(format!(
                    "unexpected status: {}",
                    String::from_utf8_lossy(&head[..head.len().min(40)])
                )));
            }
            let total = head_end + 4 + content_length;
            if avail.len() >= total {
                *start += total;
                return Ok(());
            }
        }
        // Need more data.
        if *start > 0 && *start == buf.len() {
            buf.clear();
            *start = 0;
        }
        let len = buf.len();
        buf.resize(len + 64 * 1024, 0);
        let n = stream.read(&mut buf[len..])?;
        buf.truncate(len + n);
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
}

struct Stats {
    latencies_us: Vec<u32>,
    requests: u64,
    errors: u64,
}

fn run_connection(args: Arc<Args>, measuring: Arc<AtomicBool>, stop: Arc<AtomicBool>) -> Stats {
    let mut stats = Stats {
        latencies_us: Vec::with_capacity(1 << 16),
        requests: 0,
        errors: 0,
    };
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: weft-loadgen\r\n\r\n",
        args.path, args.addr
    );
    let batch: Vec<u8> = request.repeat(args.pipeline).into_bytes();
    'outer: while !stop.load(Ordering::Relaxed) {
        let Ok(mut stream) = TcpStream::connect(args.addr) else {
            stats.errors += 1;
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
        let mut buf = Vec::with_capacity(64 * 1024);
        let mut start = 0;
        while !stop.load(Ordering::Relaxed) {
            let t0 = Instant::now();
            if stream.write_all(&batch).is_err() {
                stats.errors += 1;
                continue 'outer;
            }
            for _ in 0..args.pipeline {
                if read_response(&mut stream, &mut buf, &mut start).is_err() {
                    stats.errors += 1;
                    continue 'outer;
                }
            }
            if measuring.load(Ordering::Relaxed) {
                let us = t0.elapsed().as_micros().min(u32::MAX as u128) as u32;
                stats.latencies_us.push(us);
                stats.requests += args.pipeline as u64;
            }
        }
    }
    stats
}

fn percentile(sorted: &[u32], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx] as f64 / 1000.0
}

fn main() {
    let args = Arc::new(parse_args());
    let measuring = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let threads: Vec<_> = (0..args.connections)
        .map(|_| {
            let (a, m, s) = (args.clone(), measuring.clone(), stop.clone());
            std::thread::spawn(move || run_connection(a, m, s))
        })
        .collect();
    std::thread::sleep(args.warmup);
    measuring.store(true, Ordering::Relaxed);
    let start = Instant::now();
    std::thread::sleep(args.duration);
    measuring.store(false, Ordering::Relaxed);
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);

    let mut all = Vec::new();
    let mut requests = 0;
    let mut errors = 0;
    for t in threads {
        let s = t.join().unwrap();
        all.extend(s.latencies_us);
        requests += s.requests;
        errors += s.errors;
    }
    all.sort_unstable();
    let rps = requests as f64 / elapsed.as_secs_f64();
    println!(
        "{} {} | conns={} pipeline={} duration={:.1}s",
        args.addr,
        args.path,
        args.connections,
        args.pipeline,
        elapsed.as_secs_f64()
    );
    println!("requests={requests} errors={errors} rps={rps:.0}");
    println!(
        "latency_ms (per {}): p50={:.3} p90={:.3} p99={:.3} p99.9={:.3} max={:.3}",
        if args.pipeline == 1 {
            "request".to_string()
        } else {
            format!("batch of {}", args.pipeline)
        },
        percentile(&all, 0.50),
        percentile(&all, 0.90),
        percentile(&all, 0.99),
        percentile(&all, 0.999),
        all.last().copied().unwrap_or(0) as f64 / 1000.0
    );
}
