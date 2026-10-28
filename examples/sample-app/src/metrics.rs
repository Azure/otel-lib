// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use log::info;
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, ObservableGauge, UpDownCounter},
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    LazyLock,
};

/// A struct that contains the static metrics for the app, that is available across the code base.
/// Follow the guidelines specified here for naming - <https://opentelemetry.io/docs/specs/semconv/general/attribute-naming/>
pub static STATIC_METRICS: LazyLock<StaticMetrics> = LazyLock::new(StaticMetrics::default);
pub static OBSERVABLE_GAUGE_VALUE: AtomicU64 = AtomicU64::new(0);
pub static OBSERVABLE_GAUGE_SET: AtomicBool = AtomicBool::new(false);
const METER_NAME: &str = "sample.app";

// TODO: Consider having a StaticMetrics struct baked into the library instead of having each consumer define them,
// with the intent of driving consistent metrics across each consumer.

#[derive(Debug)]
pub struct StaticMetrics {
    pub requests: Counter<u64>,
    pub request_sizes: Histogram<u64>,
    pub request_sizes_f64: Histogram<f64>,
    pub connection_errors: Counter<u64>,
    pub updown_counter: UpDownCounter<f64>,
    pub _observable_gauge: ObservableGauge<u64>,
}

impl Default for StaticMetrics {
    fn default() -> Self {
        info!("initializing static metrics");
        let meter = global::meter_provider().meter(METER_NAME);
        StaticMetrics {
            requests: meter.u64_counter("requests").build(),
            request_sizes: meter.u64_histogram("requestsizes").build(),
            request_sizes_f64: meter.f64_histogram("requestsizes.f64").build(),
            connection_errors: meter.u64_counter("connectionerrors").build(),
            updown_counter: meter.f64_up_down_counter("updown_counter").build(),
            _observable_gauge: meter
                .u64_observable_gauge("observable_guage")
                .with_callback(|observer| {
                    if OBSERVABLE_GAUGE_SET.load(Ordering::Acquire) {
                        observer.observe(OBSERVABLE_GAUGE_VALUE.load(Ordering::Relaxed), &[]);
                    }
                })
                .build(),
        }
    }
}
