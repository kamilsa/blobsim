//! Structured event log stream for the analysis notebooks.
//!
//! Emits machine-parseable `EVENT ...` lines on the dedicated `event` tracing
//! target. Each line is timestamped with the *shared* Shadow virtual clock
//! (`SystemTime` → `CLOCK_REALTIME`), which every host reads from the same
//! simulated source — so subtracting a per-run epoch yields comparable
//! cross-node timing (the ms-level latencies the notebooks plot). Slot pacing
//! keeps using `tokio::time::Instant` (`CLOCK_MONOTONIC`, per-process zero);
//! that clock must NOT be used here.
//!
//! Line grammar (consumed by `notebooks/loaders.py`):
//!
//! ```text
//! EVENT kind=<kind> t_ms=<u64> slot=<u64> <key>=<value> ...
//! ```
//!
//! Values are bare scalars, `true`/`false`, bracketed lists `[a,b,c]`, or the
//! sentinel `NA` for an unset optional. Fields are whitespace-separated; the
//! node id is the host directory name (the log file), never a field.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch on the shared (Shadow-simulated) realtime
/// clock.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Display wrapper for an optional millisecond timestamp: prints the number, or
/// `NA` when absent (a right-censored observation the notebook reads as `NaN`).
pub struct OptMs(pub Option<u64>);

impl fmt::Display for OptMs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(v) => write!(f, "{v}"),
            None => f.write_str("NA"),
        }
    }
}

/// Format a slice of 32-byte hashes as a bracketed list of hex ids:
/// `[aabb..,ccdd..]`. Whitespace-free so it parses as one token.
pub fn hash_list(hashes: &[[u8; 32]]) -> String {
    let mut s = String::from("[");
    for (i, h) in hashes.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&hex_bytes(h));
    }
    s.push(']');
    s
}

/// Lowercase hex of an arbitrary byte slice — the stable blob id used across
/// nodes is `hex_bytes` of the 32-byte blob hash.
pub fn hex_bytes(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// Emit one structured event line at the `event` target.
///
/// Usage: `event!("slot_start", slot);` or with trailing `key = value` pairs:
/// `event!("slot_end", slot, cl_peers = n, el_peers = m);`. The `kind` must be a
/// string literal so the format string is assembled at compile time. Values are
/// formatted with `Display`.
#[macro_export]
macro_rules! event {
    ($kind:literal, $slot:expr $(, $key:ident = $val:expr)* $(,)?) => {
        ::tracing::info!(
            target: "event",
            concat!("EVENT kind=", $kind, " t_ms={} slot={}" $(, " ", stringify!($key), "={}")*),
            $crate::events::now_ms(),
            $slot,
            $($val),*
        );
    };
}
