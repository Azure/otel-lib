// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use opentelemetry::logs::Severity;
use opentelemetry_sdk::metrics::data::Temporality;
use serde::Deserialize;

#[derive(Clone, Debug)]

/// Observability configuration
pub struct Config {
    /// name of the component, for example "App"
    pub service_name: String,

    /// enterprise number, as specified here [Private Enterprise Numbers](https://www.iana.org/assignments/enterprise-numbers/)
    pub enterprise_number: Option<String>,

    /// Optional resource attributes
    pub resource_attributes: Option<Vec<Attribute>>,

    /// Optional prometheus configuration if metrics are needed in Prometheus format as well as Otel.
    pub prometheus_config: Option<Prometheus>,
    /// 0 or more metric export targets.
    pub metrics_export_targets: Option<Vec<MetricsExportTarget>>,
    /// 0 or more log export targets
    pub log_export_targets: Option<Vec<LogsExportTarget>>,
    /// set to true if metrics should be emitted to stdout.
    pub emit_metrics_to_stdout: bool,
    /// set to true if metrics should be emitted to stderr.
    pub emit_logs_to_stderr: bool,
    /// log level, specified as logging directives and controllable on a per-module basis
    pub level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            service_name: "App".to_owned(),
            enterprise_number: None,
            prometheus_config: None,
            metrics_export_targets: None,
            log_export_targets: None,
            emit_metrics_to_stdout: false,
            emit_logs_to_stderr: true,
            level: "info".to_owned(),
            resource_attributes: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
/// Prometheus configuration, which if specified results in an HTTP endpoint that can be used to get metrics
pub struct Prometheus {
    /// The port for the HTTP end point
    pub port: u16,
}

impl Default for Prometheus {
    fn default() -> Self {
        Prometheus { port: 9600 }
    }
}

#[derive(Clone, Debug)]
/// A Metrics export target definition
pub struct MetricsExportTarget {
    /// Address of the OTEL compatible repository
    pub url: String,
    /// How often to export, specified in seconds
    pub interval_secs: u64,
    /// export timeout - how long to wait before timing out on a push to the target.
    pub timeout: u64,
    /// export temporality preference, defaults to cumulative if not specified.
    pub temporality: Option<Temporality>,
}

#[derive(Clone, Debug)]
/// A Logs export target definition
pub struct LogsExportTarget {
    /// Address of the OTEL compatible repository
    pub url: String,
    /// How often to export, specified in seconds
    pub interval_secs: u64,
    /// export timeout - how long to wait before timing out on a push to the target.
    pub timeout: u64,
    /// export severity - severity >= which to export
    pub export_severity: Option<Severity>,
}

#[derive(Clone, Debug)]
pub struct Attribute {
    pub key: String,
    pub value: String,
}
