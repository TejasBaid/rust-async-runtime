//! Runtime micro-benchmarks: weft vs Tokio, same workloads, same number of
//! worker threads.
//!
//! ```text
//! cargo bench -p weft-bench --bench runtime
//! ```

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::time::Duration;

const WORKERS: usize = 4;

fn weft_rt() -> weft::Runtime {
    weft::Runtime::builder()
        .worker_threads(WORKERS)
        .build()
        .unwrap()
}

fn tokio_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .unwrap()
}

/// Spawn N empty tasks from inside a task and join them all.
fn spawn_many(c: &mut Criterion) {
    const N: usize = 10_000;
    let mut g = c.benchmark_group("spawn_many");
    g.throughput(Throughput::Elements(N as u64));
    let rt = weft_rt();
    g.bench_function(BenchmarkId::new("weft", N), |b| {
        b.iter(|| {
            rt.block_on(async {
                weft::spawn(async {
                    let handles: Vec<_> = (0..N).map(|i| weft::spawn(async move { i })).collect();
                    for h in handles {
                        h.await.unwrap();
                    }
                })
                .await
                .unwrap();
            })
        })
    });
    drop(rt);
    let rt = tokio_rt();
    g.bench_function(BenchmarkId::new("tokio", N), |b| {
        b.iter(|| {
            rt.block_on(async {
                tokio::spawn(async {
                    let handles: Vec<_> = (0..N).map(|i| tokio::spawn(async move { i })).collect();
                    for h in handles {
                        h.await.unwrap();
                    }
                })
                .await
                .unwrap();
            })
        })
    });
    g.finish();
}

/// Each task re-spawns the next: measures spawn + schedule latency along a
/// dependency chain (no parallelism available).
fn chained_spawn(c: &mut Criterion) {
    const DEPTH: usize = 1_000;
    let mut g = c.benchmark_group("chained_spawn");
    g.throughput(Throughput::Elements(DEPTH as u64));

    fn weft_chain(n: usize, done: weft::sync::oneshot::Sender<()>) {
        if n == 0 {
            done.send(()).unwrap();
        } else {
            weft::spawn(async move { weft_chain(n - 1, done) });
        }
    }
    fn tokio_chain(n: usize, done: tokio::sync::oneshot::Sender<()>) {
        if n == 0 {
            done.send(()).unwrap();
        } else {
            tokio::spawn(async move { tokio_chain(n - 1, done) });
        }
    }

    let rt = weft_rt();
    g.bench_function(BenchmarkId::new("weft", DEPTH), |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, rx) = weft::sync::oneshot::channel();
                weft::spawn(async move { weft_chain(DEPTH, tx) });
                rx.await.unwrap();
            })
        })
    });
    drop(rt);
    let rt = tokio_rt();
    g.bench_function(BenchmarkId::new("tokio", DEPTH), |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, rx) = tokio::sync::oneshot::channel();
                tokio::spawn(async move { tokio_chain(DEPTH, tx) });
                rx.await.unwrap();
            })
        })
    });
    g.finish();
}

/// T tasks each yield Y times: pure wake/re-schedule throughput.
fn yield_many(c: &mut Criterion) {
    const TASKS: usize = 200;
    const YIELDS: usize = 200;
    let mut g = c.benchmark_group("yield_many");
    g.throughput(Throughput::Elements((TASKS * YIELDS) as u64));
    let rt = weft_rt();
    g.bench_function(BenchmarkId::new("weft", TASKS * YIELDS), |b| {
        b.iter(|| {
            rt.block_on(async {
                let hs: Vec<_> = (0..TASKS)
                    .map(|_| {
                        weft::spawn(async {
                            for _ in 0..YIELDS {
                                weft::task::yield_now().await;
                            }
                        })
                    })
                    .collect();
                for h in hs {
                    h.await.unwrap();
                }
            })
        })
    });
    drop(rt);
    let rt = tokio_rt();
    g.bench_function(BenchmarkId::new("tokio", TASKS * YIELDS), |b| {
        b.iter(|| {
            rt.block_on(async {
                let hs: Vec<_> = (0..TASKS)
                    .map(|_| {
                        tokio::spawn(async {
                            for _ in 0..YIELDS {
                                tokio::task::yield_now().await;
                            }
                        })
                    })
                    .collect();
                for h in hs {
                    h.await.unwrap();
                }
            })
        })
    });
    g.finish();
}

/// Two tasks bounce a message over a pair of capacity-1 mpsc channels.
fn mpsc_ping_pong(c: &mut Criterion) {
    const ROUND_TRIPS: usize = 10_000;
    let mut g = c.benchmark_group("mpsc_ping_pong");
    g.throughput(Throughput::Elements(ROUND_TRIPS as u64));
    let rt = weft_rt();
    g.bench_function(BenchmarkId::new("weft", ROUND_TRIPS), |b| {
        b.iter(|| {
            rt.block_on(async {
                weft::spawn(async {
                    let (ping_tx, mut ping_rx) = weft::sync::mpsc::channel::<usize>(1);
                    let (pong_tx, mut pong_rx) = weft::sync::mpsc::channel::<usize>(1);
                    let ponger = weft::spawn(async move {
                        while let Some(v) = ping_rx.recv().await {
                            pong_tx.send(v).await.unwrap();
                        }
                    });
                    for i in 0..ROUND_TRIPS {
                        ping_tx.send(i).await.unwrap();
                        assert_eq!(pong_rx.recv().await, Some(i));
                    }
                    drop(ping_tx);
                    ponger.await.unwrap();
                })
                .await
                .unwrap();
            })
        })
    });
    drop(rt);
    let rt = tokio_rt();
    g.bench_function(BenchmarkId::new("tokio", ROUND_TRIPS), |b| {
        b.iter(|| {
            rt.block_on(async {
                tokio::spawn(async {
                    let (ping_tx, mut ping_rx) = tokio::sync::mpsc::channel::<usize>(1);
                    let (pong_tx, mut pong_rx) = tokio::sync::mpsc::channel::<usize>(1);
                    let ponger = tokio::spawn(async move {
                        while let Some(v) = ping_rx.recv().await {
                            pong_tx.send(v).await.unwrap();
                        }
                    });
                    for i in 0..ROUND_TRIPS {
                        ping_tx.send(i).await.unwrap();
                        assert_eq!(pong_rx.recv().await, Some(i));
                    }
                    drop(ping_tx);
                    ponger.await.unwrap();
                })
                .await
                .unwrap();
            })
        })
    });
    g.finish();
}

/// Ablation: the same ping-pong with weft's LIFO slot disabled, to measure
/// what the optimisation buys on message-passing workloads.
fn lifo_ablation(c: &mut Criterion) {
    const ROUND_TRIPS: usize = 10_000;
    let mut g = c.benchmark_group("lifo_ablation_ping_pong");
    g.throughput(Throughput::Elements(ROUND_TRIPS as u64));
    for lifo in [true, false] {
        let mut builder = weft::Runtime::builder();
        builder.worker_threads(WORKERS);
        if !lifo {
            builder.disable_lifo_slot();
        }
        let rt = builder.build().unwrap();
        let name = if lifo { "lifo_on" } else { "lifo_off" };
        g.bench_function(BenchmarkId::new(name, ROUND_TRIPS), |b| {
            b.iter(|| {
                rt.block_on(async {
                    weft::spawn(async {
                        let (ping_tx, mut ping_rx) = weft::sync::mpsc::channel::<usize>(1);
                        let (pong_tx, mut pong_rx) = weft::sync::mpsc::channel::<usize>(1);
                        let ponger = weft::spawn(async move {
                            while let Some(v) = ping_rx.recv().await {
                                pong_tx.send(v).await.unwrap();
                            }
                        });
                        for i in 0..ROUND_TRIPS {
                            ping_tx.send(i).await.unwrap();
                            assert_eq!(pong_rx.recv().await, Some(i));
                        }
                        drop(ping_tx);
                        ponger.await.unwrap();
                    })
                    .await
                    .unwrap();
                })
            })
        });
    }
    g.finish();
}

/// Many producers, one consumer, bounded channel.
fn mpsc_contended(c: &mut Criterion) {
    const PRODUCERS: usize = 8;
    const PER_PRODUCER: usize = 2_000;
    let mut g = c.benchmark_group("mpsc_contended");
    g.throughput(Throughput::Elements((PRODUCERS * PER_PRODUCER) as u64));
    let rt = weft_rt();
    g.bench_function(BenchmarkId::new("weft", PRODUCERS * PER_PRODUCER), |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = weft::sync::mpsc::channel::<usize>(128);
                for _ in 0..PRODUCERS {
                    let tx = tx.clone();
                    weft::spawn(async move {
                        for i in 0..PER_PRODUCER {
                            tx.send(i).await.unwrap();
                        }
                    });
                }
                drop(tx);
                let mut n = 0;
                while rx.recv().await.is_some() {
                    n += 1;
                }
                assert_eq!(n, PRODUCERS * PER_PRODUCER);
            })
        })
    });
    drop(rt);
    let rt = tokio_rt();
    g.bench_function(BenchmarkId::new("tokio", PRODUCERS * PER_PRODUCER), |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<usize>(128);
                for _ in 0..PRODUCERS {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        for i in 0..PER_PRODUCER {
                            tx.send(i).await.unwrap();
                        }
                    });
                }
                drop(tx);
                let mut n = 0;
                while rx.recv().await.is_some() {
                    n += 1;
                }
                assert_eq!(n, PRODUCERS * PER_PRODUCER);
            })
        })
    });
    g.finish();
}

/// Spawn a task that sends on a oneshot; await it. Repeated sequentially.
fn oneshot_round_trip(c: &mut Criterion) {
    const N: usize = 1_000;
    let mut g = c.benchmark_group("oneshot_spawn_round_trip");
    g.throughput(Throughput::Elements(N as u64));
    let rt = weft_rt();
    g.bench_function(BenchmarkId::new("weft", N), |b| {
        b.iter(|| {
            rt.block_on(async {
                weft::spawn(async {
                    for i in 0..N {
                        let (tx, rx) = weft::sync::oneshot::channel();
                        weft::spawn(async move { tx.send(i).unwrap() });
                        assert_eq!(rx.await.unwrap(), i);
                    }
                })
                .await
                .unwrap();
            })
        })
    });
    drop(rt);
    let rt = tokio_rt();
    g.bench_function(BenchmarkId::new("tokio", N), |b| {
        b.iter(|| {
            rt.block_on(async {
                tokio::spawn(async {
                    for i in 0..N {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        tokio::spawn(async move { tx.send(i).unwrap() });
                        assert_eq!(rx.await.unwrap(), i);
                    }
                })
                .await
                .unwrap();
            })
        })
    });
    g.finish();
}

fn config() -> Criterion {
    Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(4))
}

criterion_group! {
    name = benches;
    config = config();
    targets = spawn_many, chained_spawn, yield_many, mpsc_ping_pong, lifo_ablation, mpsc_contended, oneshot_round_trip
}
criterion_main!(benches);
