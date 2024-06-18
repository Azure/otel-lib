// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use clap::{arg, command, Parser};
use log::{error, info};
use opentelemetry::logs::Severity;
use opentelemetry_sdk::metrics::data::Temporality;
use otel_lib::{
    config::{Attribute, Config, LogsExportTarget, MetricsExportTarget},
    Otel,
};

use rand::Rng;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;

use crate::metrics::STATIC_METRICS;

mod metrics;

#[tokio::main]
async fn main() {
    // App expects 2 parameters. 1) '-n' that controls the number of iterations, and 2) '-o' to specify an otel compatible repo.
    let args = Args::parse();

    let (metrics_targets, logs_targets) = match args.otel_repo_url {
        Some(url) => {
            let metric_targets = vec![MetricsExportTarget {
                url: url.clone(),
                interval_secs: 1,
                timeout: 5,
                temporality: Some(Temporality::Cumulative),
            }];
            let logs_targets = vec![LogsExportTarget {
                url,
                interval_secs: 1,
                timeout: 5,
                export_severity: Some(Severity::Info),
            }];
            (Some(metric_targets), Some(logs_targets))
        }
        None => (None, None),
    };

    let config = Config {
        emit_metrics_to_stdout: true,
        metrics_export_targets: metrics_targets,
        log_export_targets: logs_targets,
        level: "info,hyper=off".to_owned(),
        service_name: "sample-app".to_owned(),
        resource_attributes: Some(vec![Attribute {
            key: "resource_key1".to_owned(),
            value: "1".to_owned(),
        }]),
        ..Config::default()
    };

    let otel_component = Otel::new(config);
    // Start the otel running task
    let otel_long_running_task = otel_component.run();
    // initialize static metrics
    let _ = STATIC_METRICS.requests;

    error!("Test error log. Only this log will be exported to the target");

    // Run this loop for n iterations
    let instrumentation_task = tokio::spawn(async move {
        for iteration in 1..args.num_iterations {
            STATIC_METRICS.requests.add(1, &[]);
            STATIC_METRICS.request_sizes.record(25, &[]);
            let mut val: f64 = rand::thread_rng().gen();
            val *= 1_000_000.0;
            STATIC_METRICS.request_sizes_f64.record(val, &[]);
            STATIC_METRICS.connection_errors.add(1, &[]);
            // Randomly add a positive or negative value to the updown counter
            if rand::random() {
                STATIC_METRICS.updown_counter.add(val, &[]);
            } else {
                STATIC_METRICS.updown_counter.add(val * -1.0, &[]);
            }
            STATIC_METRICS.observable_gauge.observe(iteration, &[]);
            info!("iteration: {iteration}");
            sleep(Duration::from_micros(100)).await;
        }
    });

    select! {
        _ = instrumentation_task => {},
        handle = otel_long_running_task => { error!("otel long running task ended unexpectedly: {:?}", handle)}
    }
    otel_component.shutdown();
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of iterations
    #[arg(short, long, default_value_t = 1000)]
    pub num_iterations: u64,

    /// Otel Repository URL
    #[arg(short, long)]
    pub otel_repo_url: Option<String>,
}
