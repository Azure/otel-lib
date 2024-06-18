# Project

A simple framework that helps setup and export metrics, logs, (and in the future distributed tracing) using Open Telemetry for your component.
Logs are instrumented using the standard [log crate macros] (https://docs.rs/log/latest/log/), written to `stderr` in the [syslog format](https://www.rfc-editor.org/rfc/rfc5424#page-8), and available to exported to log repositories that support OTLP/gRPC.
Metrics are instrumented using the [open telemetry sdk] (https://crates.io/crates/opentelemetry), can be converted to Prometheus, and available to be exported to metric repositories that support OTLP/gRPC.

The project also includes a sample app that demonstrates how to use the framework.

### Configuration
The framework is configurable using the `Config` struct to setup
* service name
* 0 or more metrics export targets, where each target is a metrics repository that supports OTLP/gRPC
* Enable Prometheus support. When enabled, the framework will translate the instrumented OTEL metrics into Prometheus metrics and provide a HTTP endpoint that can be scraped by external Prometheus scrapers
* Enable metrics to be emitted to stdout. These will show up as pretty printed JSON
* 0 or more Log export targets, where each target is a log repository that supports OTLP/gRPC.
* Enable logs to be emitted to stderr. These will show up as logs in the [syslog format](https://www.rfc-editor.org/rfc/rfc5424#page-8).

#### How to set it up
Do the following as early as you can in your control flow
~~~
// Configure
let metric_targets = vec![MetricsExportTarget {
        url: "http://localhost:4317".to_string(),
        interval_secs: 30,
        timeout: 15,
        delta_temporality: Temporality::Cumulative, // Set to one of Some(Temporality::Cumulative) or Some(Temporality::Delta) or None (while defaults to cumulative)
    }];

let log_targets = vec![LogsExportTarget {
    url: "http://localhost:4317".to_string(),
    interval_secs: 10,
    timeout: 15,
    export_severity: Some(Severity::Error), // Applies an additional filter at the exporter level. This can be set to `None` if no additional filtering is required.
}];

let config = Config {
    service_name: "myapp".to_owned(),
    emit_metrics_to_stdout: true,
    emit_logs_to_stderr: true,
    metrics_export_targets: Some(metric_targets),
    log_export_targets: Some(log_targets),
    level: "info,hyper=off".to_owned(),
    resource_attributes: Some(vec![Attribute {
        key: "resource_key1".to_owned(),
        value: "1".to_owned(),
    }]),
    ..Config::default()
};
~~~

#### Initialize and run
~~~
let otel_long_running_task = Otel::new(config).run();
~~~

// Drive the task using something like
~~~
 _ = tokio::join!(otel_long_running_task);
~~~

This initializes a static item STATIC_METRICS of type StaticMetrics that you can tweak to instrument metrics for you code.

#### Instrument metrics
~~~
// Add a metric to StaticMetrics
pub struct StaticMetrics {
    pub requests: Counter<u64>,
    ...

// Initialize the metric
    let meter = global::meter_provider().meter(METER_NAME);
    StaticMetrics {
        requests: meter.u64_counter("requests").init(),
        ...
    }

// add a data point where needed
 STATIC_METRICS.requests.add(1, &[]);
~~~

#### Instrument Logs
For log instrumentation, use the standard log::crate macros.

#### Instrument Traces
Traces: TBD

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
