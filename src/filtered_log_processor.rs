// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::runtime::{RuntimeChannel, TrySend};
use futures_channel::oneshot;
use futures_util::{
    future::{self, Either},
    {pin_mut, stream, StreamExt as _},
};
#[cfg(feature = "logs_level_enabled")]
use opentelemetry::logs::Severity;
use opentelemetry::{
    global,
    logs::{LogError, LogResult, Severity},
};
use opentelemetry_sdk::{
    export::logs::{ExportResult, LogData, LogExporter},
    logs::LogProcessor,
};

use std::{
    fmt::{self, Debug, Formatter},
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

/// A [`LogProcessor`] that asynchronously buffers log records and reports
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
    fn emit(&self, data: LogData) {
        let result = self.message_sender.try_send(BatchMessage::ExportLog(data));

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

    fn shutdown(&mut self) -> LogResult<()> {
        let (res_sender, res_receiver) = oneshot::channel();
        self.message_sender
            .try_send(BatchMessage::Shutdown(res_sender))
            .map_err(|err| LogError::Other(err.into()))?;

        futures_executor::block_on(res_receiver)
            .map_err(|err| LogError::Other(err.into()))
            .and_then(std::convert::identity)
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
                        // add log only if the severity is >= export_severity
                        if let Some(severity) = log.record.severity_number {
                            if severity >= config.export_severity {
                                logs.push(log);
                            } else {
                                continue;
                            }
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
            batch_config: Default::default(),
            runtime,
        }
    }
}

async fn export_with_timeout<R, E>(
    time_out: Duration,
    exporter: &mut E,
    runtime: &R,
    batch: Vec<LogData>,
) -> ExportResult
where
    R: RuntimeChannel,
    E: LogExporter + ?Sized,
{
    if batch.is_empty() {
        return Ok(());
    }

    let export = exporter.export(batch);
    let timeout = runtime.delay(time_out);
    pin_mut!(export);
    pin_mut!(timeout);
    match future::select(export, timeout).await {
        Either::Left((export_res, _)) => export_res,
        Either::Right((_, _)) => ExportResult::Err(LogError::ExportTimedOut(time_out)),
    }
}

#[derive(Debug, Clone, Copy)]
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
}

impl Default for FilteredBatchConfig {
    fn default() -> Self {
        Self {
            max_queue_size: OTEL_BLRP_MAX_QUEUE_SIZE_DEFAULT,
            scheduled_delay: Duration::from_millis(OTEL_BLRP_SCHEDULE_DELAY_DEFAULT),
            max_export_batch_size: OTEL_BLRP_MAX_EXPORT_BATCH_SIZE_DEFAULT,
            max_export_timeout: Duration::from_millis(OTEL_BLRP_EXPORT_TIMEOUT_DEFAULT),
            export_severity: Severity::Error,
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
}
