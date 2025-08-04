// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// This implementation is a slight adjustment of the code at
// [BatchLogProcessor](https://github.com/open-telemetry/opentelemetry-rust/blob/b933bdd82dadbadedb42a1f572caba1e0b8b9391/opentelemetry-sdk/src/logs/log_processor.rs#L154)
//
// I've opened an issue on the opentelemetry_rust SDK repo: [1881](https://github.com/open-telemetry/opentelemetry-rust/issues/1881).
// If that issue is accepted and addressed, this implementation will no longer be required.

use crate::runtime::{RuntimeChannel, TrySend};
use futures_channel::oneshot;
use futures_util::{
    future::{self, Either},
    {pin_mut, stream, StreamExt as _},
};

use opentelemetry::{
    global,
    logs::{AnyValue, LogError, LogResult, Severity},
};
use opentelemetry_sdk::{
    export::logs::{ExportResult, LogData, LogExporter},
    logs::LogProcessor,
    Resource,
};

use std::{
    borrow::Cow,
    fmt::{self, Debug, Formatter},
    sync::Arc,
    time::Duration,
};

/// Default delay interval between two consecutive exports.
const OTEL_BLRP_SCHEDULE_DELAY_DEFAULT: u64 = 1_000;
/// Maximum allowed time to export data.
const OTEL_BLRP_EXPORT_TIMEOUT_DEFAULT: u64 = 30_000;
/// Default maximum queue size.
const OTEL_BLRP_MAX_QUEUE_SIZE_DEFAULT: usize = 2_048;
/// Default maximum batch size.
const OTEL_BLRP_MAX_EXPORT_BATCH_SIZE_DEFAULT: usize = 512;

/// A [`LogProcessor`] that asynchronously buffers log records, applies a severity filter, and exports
/// them at a pre-configured interval.
pub struct FilteredBatchLogProcessor<R: RuntimeChannel> {
    message_sender: R::Sender<BatchMessage>,
}

impl<R: RuntimeChannel> Debug for FilteredBatchLogProcessor<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilteredBatchLogProcessor")
            .field("message_sender", &self.message_sender)
            .finish()
    }
}

impl<R: RuntimeChannel> LogProcessor for FilteredBatchLogProcessor<R> {
    fn emit(&self, data: &mut LogData) {
        let result = self
            .message_sender
            .try_send(BatchMessage::ExportLog(data.clone()));

        if let Err(err) = result {
            global::handle_error(LogError::Other(err.into()));
        }
    }

    fn force_flush(&self) -> LogResult<()> {
        let (res_sender, res_receiver) = oneshot::channel();
        self.message_sender
            .try_send(BatchMessage::Flush(Some(res_sender)))
            .map_err(|err| LogError::Other(err.into()))?;

        futures_executor::block_on(res_receiver)
            .map_err(|err| LogError::Other(err.into()))
            .and_then(std::convert::identity)
    }

    fn shutdown(&self) -> LogResult<()> {
        let (res_sender, res_receiver) = oneshot::channel();
        self.message_sender
            .try_send(BatchMessage::Shutdown(res_sender))
            .map_err(|err| LogError::Other(err.into()))?;

        futures_executor::block_on(res_receiver)
            .map_err(|err| LogError::Other(err.into()))
            .and_then(std::convert::identity)
    }

    fn set_resource(&self, resource: &Resource) {
        let resource = Arc::new(resource.clone());
        let _ = self
            .message_sender
            .try_send(BatchMessage::SetResource(resource));
    }

    fn event_enabled(
        &self,
        _level: opentelemetry::logs::Severity,
        _target: &str,
        _name: &str,
    ) -> bool {
        true
    }
}

impl<R: RuntimeChannel> FilteredBatchLogProcessor<R> {
    pub(crate) fn new(
        mut exporter: Box<dyn LogExporter>,
        config: FilteredBatchConfig,
        runtime: &R,
    ) -> Self {
        let (message_sender, message_receiver) =
            runtime.batch_message_channel(config.max_queue_size);
        let ticker = runtime
            .interval(config.scheduled_delay)
            .map(|_| BatchMessage::Flush(None));
        let timeout_runtime = runtime.clone();

        // Spawn worker process via user-defined spawn function.
        runtime.spawn(Box::pin(async move {
            let mut logs = Vec::new();
            let mut messages = Box::pin(stream::select(message_receiver, ticker));

            while let Some(message) = messages.next().await {
                match message {
                    BatchMessage::ExportLog(log) => {
                        // apply both severity and target filtering
                        let severity_matches = if let Some(severity) = log.record.severity_number {
                            severity >= config.export_severity
                        } else {
                            false
                        };

                        let target_matches = if let Some(ref target_filter) = config.target_filter {
                            // Check if the log has a "target" attribute that matches our filter
                            log.record.attributes.as_ref().is_some_and(|attrs| {
                                attrs.iter().any(|(key, value)| {
                                    if key.as_str() == "target" {
                                        // Extract string value from AnyValue by matching on the enum
                                        match value {
                                            AnyValue::String(target_value) => {
                                                target_value.as_str() == target_filter
                                            }
                                            _ => false,
                                        }
                                    } else {
                                        false
                                    }
                                })
                            })
                        } else {
                            true // if no target filter specified, accept all logs
                        };

                        if severity_matches && target_matches {
                            logs.push(Cow::Owned(log));
                        } else {
                            continue;
                        }

                        if logs.len() == config.max_export_batch_size {
                            let result = export_with_timeout(
                                config.max_export_timeout,
                                exporter.as_mut(),
                                &timeout_runtime,
                                logs.split_off(0),
                            )
                            .await;

                            if let Err(err) = result {
                                global::handle_error(err);
                            }
                        }
                    }
                    // Log batch interval time reached or a force flush has been invoked, export current spans.
                    BatchMessage::Flush(res_channel) => {
                        let result = export_with_timeout(
                            config.max_export_timeout,
                            exporter.as_mut(),
                            &timeout_runtime,
                            logs.split_off(0),
                        )
                        .await;

                        if let Some(channel) = res_channel {
                            if let Err(result) = channel.send(result) {
                                global::handle_error(LogError::from(format!(
                                    "failed to send flush result: {result:?}"
                                )));
                            }
                        } else if let Err(err) = result {
                            global::handle_error(err);
                        }
                    }
                    // Stream has terminated or processor is shutdown, return to finish execution.
                    BatchMessage::Shutdown(ch) => {
                        let result = export_with_timeout(
                            config.max_export_timeout,
                            exporter.as_mut(),
                            &timeout_runtime,
                            logs.split_off(0),
                        )
                        .await;

                        exporter.shutdown();

                        if let Err(result) = ch.send(result) {
                            global::handle_error(LogError::from(format!(
                                "failed to send batch processor shutdown result: {result:?}"
                            )));
                        }

                        break;
                    }

                    // propagate the resource
                    BatchMessage::SetResource(resource) => {
                        exporter.set_resource(&resource);
                    }
                }
            }
        }));

        // Return batch processor with link to worker
        FilteredBatchLogProcessor { message_sender }
    }

    /// Create a new batch processor builder
    pub(crate) fn builder<E>(exporter: E, runtime: R) -> FilteredBatchLogProcessorBuilder<E, R>
    where
        E: LogExporter,
    {
        FilteredBatchLogProcessorBuilder {
            exporter,
            batch_config: FilteredBatchConfig::default(),
            runtime,
        }
    }
}

async fn export_with_timeout<R, E>(
    time_out: Duration,
    exporter: &mut E,
    runtime: &R,
    batch: Vec<Cow<'_, LogData>>,
) -> ExportResult
where
    R: RuntimeChannel,
    E: LogExporter + ?Sized,
{
    if batch.is_empty() {
        return Ok(());
    }

    let export = exporter.export(batch);
    let delay = runtime.delay(time_out);
    pin_mut!(export);
    pin_mut!(delay);
    match future::select(export, delay).await {
        Either::Left((export_res, _)) => export_res,
        Either::Right((_, _)) => ExportResult::Err(LogError::ExportTimedOut(time_out)),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FilteredBatchConfig {
    /// The maximum queue size to buffer logs for delayed processing. If the
    /// queue gets full it drops the logs. The default value of is 2048.
    pub max_queue_size: usize,

    /// The delay interval in milliseconds between two consecutive processing
    /// of batches. The default value is 1 second.
    pub scheduled_delay: Duration,

    /// The maximum number of logs to process in a single batch. If there are
    /// more than one batch worth of logs then it processes multiple batches
    /// of logs one batch after the other without any delay. The default value
    /// is 512.
    pub max_export_batch_size: usize,

    /// The maximum duration to export a batch of data.
    pub max_export_timeout: Duration,

    /// export level - levels >= which to export
    pub export_severity: Severity,

    /// target filter - only export logs from this target. If None, exports all logs.
    pub target_filter: Option<String>,
}

impl Default for FilteredBatchConfig {
    fn default() -> Self {
        Self {
            max_queue_size: OTEL_BLRP_MAX_QUEUE_SIZE_DEFAULT,
            scheduled_delay: Duration::from_millis(OTEL_BLRP_SCHEDULE_DELAY_DEFAULT),
            max_export_batch_size: OTEL_BLRP_MAX_EXPORT_BATCH_SIZE_DEFAULT,
            max_export_timeout: Duration::from_millis(OTEL_BLRP_EXPORT_TIMEOUT_DEFAULT),
            export_severity: Severity::Error,
            target_filter: None,
        }
    }
}

/// A builder for creating [`FilteredBatchLogProcessor`] instances.
///
#[derive(Debug)]
pub(crate) struct FilteredBatchLogProcessorBuilder<E, R> {
    exporter: E,
    batch_config: FilteredBatchConfig,
    runtime: R,
}

impl<E, R> FilteredBatchLogProcessorBuilder<E, R>
where
    E: LogExporter + 'static,
    R: RuntimeChannel,
{
    /// Set the `FilteredBatchConfig` for [`BatchLogProcessorBuilder`]
    pub(crate) fn with_batch_config(self, config: FilteredBatchConfig) -> Self {
        FilteredBatchLogProcessorBuilder {
            batch_config: config,
            ..self
        }
    }

    /// Build a batch processor
    pub(crate) fn build(self) -> FilteredBatchLogProcessor<R> {
        FilteredBatchLogProcessor::new(Box::new(self.exporter), self.batch_config, &self.runtime)
    }
}

/// Messages sent between application thread and batch log processor's work thread.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum BatchMessage {
    /// Export logs, usually called when the log is emitted.
    ExportLog(LogData),
    /// Flush the current buffer to the backend, it can be triggered by
    /// pre configured interval or a call to `force_push` function.
    Flush(Option<oneshot::Sender<ExportResult>>),
    /// Shut down the worker thread, push all logs in buffer to the backend.
    Shutdown(oneshot::Sender<ExportResult>),
    /// Set the resource for the exporter.
    SetResource(Arc<Resource>),
}
