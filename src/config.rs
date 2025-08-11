// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use opentelemetry::logs::Severity;
use opentelemetry_sdk::metrics::data::Temporality;
use serde::Deserialize;

/// Observability configuration
#[derive(Clone, Debug)]
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
    /// path to the ca cert
    pub ca_cert_path: Option<String>,
    /// a fn that provides the bearer token, which will be called to get the token for each export request
    pub bearer_token_provider_fn: Option<fn() -> String>,
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
    /// target name filters - only export logs that match any of these target patterns. If None, exports all logs.
    /// Supports exact matches and prefix patterns (ending with '::')
    pub target_filters: Option<Vec<String>>,
    /// path to root ca cert
    pub ca_cert_path: Option<String>,
    /// a fn that provides the bearer token, which will be called to get the token for each export request
    pub bearer_token_provider_fn: Option<fn() -> String>,
}

impl Default for LogsExportTarget {
    fn default() -> Self {
        Self {
            url: String::new(),
            interval_secs: 30,
            timeout: 5,
            export_severity: None,
            target_filters: None,
            ca_cert_path: None,
            bearer_token_provider_fn: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Attribute {
    pub key: String,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use opentelemetry::logs::Severity;
    use opentelemetry_sdk::metrics::data::Temporality;

    use super::{Attribute, Config, LogsExportTarget, MetricsExportTarget, Prometheus};

    #[test]
    fn test_default_config() {
        let default_config = Config::default();

        assert_eq!(default_config.service_name, "App");
        assert_eq!(default_config.enterprise_number, None);
        assert_eq!(default_config.prometheus_config, None);
        assert!(default_config.resource_attributes.is_none());
        assert!(default_config.metrics_export_targets.is_none());
        assert!(default_config.log_export_targets.is_none());
        assert!(!default_config.emit_metrics_to_stdout);
        assert!(default_config.emit_logs_to_stderr);
        assert_eq!(default_config.level, "info".to_owned());
    }

    #[test]
    fn test_default_prometheus_config() {
        let prometheus_config = Prometheus::default();
        assert_eq!(prometheus_config.port, 9600);
    }

    #[test]
    fn test_config() {
        let prometheus_config = Some(Prometheus { port: 9090 });
        let metric_url = "testmetricurl".to_owned();
        let log_url = "testlogurl".to_owned();

        let metric_targets = vec![MetricsExportTarget {
            url: metric_url,
            interval_secs: 1,
            timeout: 5,
            temporality: Some(Temporality::Cumulative),
            ca_cert_path: None,
            bearer_token_provider_fn: None,
        }];
        let logs_targets = vec![LogsExportTarget {
            url: log_url,
            interval_secs: 1,
            timeout: 5,
            export_severity: Some(Severity::Error),
            bearer_token_provider_fn: Some(get_dummy_auth_token),
            ..Default::default()
        }];

        let config = Config {
            emit_metrics_to_stdout: false,
            metrics_export_targets: Some(metric_targets),
            log_export_targets: Some(logs_targets),
            level: "info,hyper=off".to_owned(),
            service_name: "test-app".to_owned(),
            enterprise_number: Some("123".to_owned()),
            resource_attributes: Some(vec![Attribute {
                key: "resource_key1".to_owned(),
                value: "1".to_owned(),
            }]),
            prometheus_config,
            emit_logs_to_stderr: false,
        };

        assert_eq!(config.service_name, "test-app");
        assert_eq!(config.enterprise_number, Some("123".to_owned()));
        assert!(!config.emit_metrics_to_stdout);
        assert!(!config.emit_logs_to_stderr);
        assert_eq!(config.level, "info,hyper=off".to_owned());
    }

    fn get_dummy_auth_token() -> String {
        "test".to_owned()
    }
}
