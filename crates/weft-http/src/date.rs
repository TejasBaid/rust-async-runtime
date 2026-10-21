//! IMF-fixdate formatting for the `Date` header, cached per second and per
//! thread so the hot path is a timestamp comparison.

use std::cell::RefCell;
use std::time::{SystemTime, UNIX_EPOCH};

thread_local! {
    static CACHE: RefCell<(u64, String)> = const { RefCell::new((u64::MAX, String::new())) };
}

/// Calls `f` with the current date, e.g. `Sun, 06 Nov 1994 08:49:37 GMT`.
pub(crate) fn with_http_date<R>(f: impl FnOnce(&str) -> R) -> R {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.0 != now {
            *cache = (now, format_http_date(now));
        }
        f(&cache.1)
    })
}

/// Formats seconds since the Unix epoch as an IMF-fixdate.
pub(crate) fn format_http_date(secs: u64) -> String {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{}, {:02} {} {} {:02}:{:02}:{:02} GMT",
        DAYS[(days % 7) as usize],
        day,
        MONTHS[(month - 1) as usize],
        year,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant's days-to-civil algorithm (proleptic Gregorian calendar).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_dates() {
        assert_eq!(format_http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(
            format_http_date(784_111_777),
            "Sun, 06 Nov 1994 08:49:37 GMT"
        );
        assert_eq!(
            format_http_date(951_782_400),
            "Tue, 29 Feb 2000 00:00:00 GMT"
        );
        assert_eq!(
            format_http_date(1_700_000_000),
            "Tue, 14 Nov 2023 22:13:20 GMT"
        );
    }
}
