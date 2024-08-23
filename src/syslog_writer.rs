// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::time::SystemTime;

use humantime::format_rfc3339_millis;
use log::{Level, Record};

pub(crate) fn write_syslog_format(
    record: &Record<'_>,
    service_name: &str,
    host_name: &str,
    timestamp: &SystemTime,
) {
    // Write to stderr
    // TODO: check if there is any benefit to buffering this write, given the trade-off of missing logs if the app panics.
    let level = to_syslog_level(record.level());
    let timestamp = format_rfc3339_millis(*timestamp);
    let thread_id = nix::unistd::gettid().as_raw();

    if record.level() >= Level::Debug {
        // Only include more verbose module level on Debug and Trace logs
        eprintln!(
            r#"<{level}>{timestamp} {host_name} [{service_name} tid="{thread_id}" module="{}"] - {}"#,
            record.target(),
            record.args()
        );
    } else {
        eprintln!(
            r#"<{level}>{timestamp} {host_name} [{service_name} tid="{thread_id}"] - {}"#,
            record.args()
        );
    }
}

const fn to_syslog_level(level: log::Level) -> i8 {
    match level {
        log::Level::Error => 3,
        log::Level::Warn => 4,
        log::Level::Info => 6,
        log::Level::Debug | log::Level::Trace => 7,
    }
}
