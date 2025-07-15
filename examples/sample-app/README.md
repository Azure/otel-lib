# Observability Example App

A simple app that shows how to use the azure_iot_operations_otel to
- create a bunch of metrics,
- update them in a loop,
- export them to an open telemetry compatible collector,
- and make them available as a prometheus endpoint.

The app supports command line parameters as below
~~~
Usage: sample-app [OPTIONS]

Options:
  -n, --num-iterations <NUM_ITERATIONS>  Number of iterations [default: 1000]
  -o, --otel-repo-url <OTEL_REPO_URL>    Otel Repository URL
  -h, --help                             Print help
  -V, --version                          Print version
~~~

## How to run the app
To run the app and check that everything is working as it should,
~~~
1) Run an Otel collector locally and make it available on localhost port 4317

docker run -d -p 4317:4317 otel/opentelemetry-collector:latest

2) cargo run -- -n 3000 -o "http://localhost:4317"
This will run the metric update loop 3000 times and will export metrics and logs to the Otel collector once a second. You can see the metrics & logs being ingested by tailing the Otel Collector's logs. [Note: only ERROR logs will appear in the Otel Collector as the sample has set the export filter to ERROR]
    docker logs <CONTAINER ID> -f

3) while the app is running, do a `curl http://localhost:9600/metrics` to view the metrics in the Prometheus Format. You will see an output that looks like the following

# TYPE connectionerrors_total counter
connectionerrors_total{otel_scope_name="sample.app"} 1185
# TYPE observable_guage gauge
observable_guage{otel_scope_name="sample.app"} 1185
# TYPE requests_total counter
requests_total{otel_scope_name="sample.app"} 1185
# TYPE requestsizes histogram
requestsizes_bucket{otel_scope_name="sample.app",le="0"} 0
requestsizes_bucket{otel_scope_name="sample.app",le="5"} 0
requestsizes_bucket{otel_scope_name="sample.app",le="10"} 0
requestsizes_bucket{otel_scope_name="sample.app",le="25"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="50"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="75"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="100"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="250"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="500"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="750"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="1000"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="2500"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="5000"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="7500"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="10000"} 1185
requestsizes_bucket{otel_scope_name="sample.app",le="+Inf"} 1185
requestsizes_sum{otel_scope_name="sample.app"} 29625
requestsizes_count{otel_scope_name="sample.app"} 1185
# TYPE requestsizes_f64 histogram
requestsizes_f64_bucket{otel_scope_name="sample.app",le="0"} 0
requestsizes_f64_bucket{otel_scope_name="sample.app",le="5"} 0
requestsizes_f64_bucket{otel_scope_name="sample.app",le="10"} 0
requestsizes_f64_bucket{otel_scope_name="sample.app",le="25"} 0
requestsizes_f64_bucket{otel_scope_name="sample.app",le="50"} 0
requestsizes_f64_bucket{otel_scope_name="sample.app",le="75"} 0
requestsizes_f64_bucket{otel_scope_name="sample.app",le="100"} 0
requestsizes_f64_bucket{otel_scope_name="sample.app",le="250"} 0
requestsizes_f64_bucket{otel_scope_name="sample.app",le="500"} 0
requestsizes_f64_bucket{otel_scope_name="sample.app",le="750"} 1
requestsizes_f64_bucket{otel_scope_name="sample.app",le="1000"} 1
requestsizes_f64_bucket{otel_scope_name="sample.app",le="2500"} 1
requestsizes_f64_bucket{otel_scope_name="sample.app",le="5000"} 4
requestsizes_f64_bucket{otel_scope_name="sample.app",le="7500"} 9
requestsizes_f64_bucket{otel_scope_name="sample.app",le="10000"} 9
requestsizes_f64_bucket{otel_scope_name="sample.app",le="+Inf"} 1185
requestsizes_f64_sum{otel_scope_name="sample.app"} 591109370.6193665
requestsizes_f64_count{otel_scope_name="sample.app"} 1185
# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="sampleapp"} 1
# TYPE updown_counter gauge
updown_counter{otel_scope_name="sample.app"} -31499309.99587886
~~~
