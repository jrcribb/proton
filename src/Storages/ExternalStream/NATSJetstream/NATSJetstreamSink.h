#pragma once

#include "config.h"

#if USE_NATS

#include <Processors/Executors/MessageQueueFormatExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>

#include <nats/nats.h>

#include <atomic>

namespace DB::ExternalStream
{

class NATSJetstream;

/// Sink for publishing data to NATS JetStream subjects.
///
/// Implements synchronous publishing with acknowledgement tracking:
/// - Supports _tp_message_key for dynamic subject routing
/// - Supports _tp_message_headers for setting NATS message headers
/// - Checkpoint ensures all outstanding messages are ACKed by the server
/// - Duplicate message detection using publisher ACKs
///
class NATSJetstreamSink final : public SinkToStorage
{
public:
    NATSJetstreamSink(
        NATSJetstream & storage,
        const Block & header,
        ExternalStreamCounterPtr external_stream_counter_,
        LoggerPtr logger_,
        ContextPtr context);

    ~NATSJetstreamSink() override;

    String getName() const override { return "NATSJetstreamSink"; }

    void consume(Chunk chunk) override;
    void onFinish() override;
    void checkpoint(CheckpointContextPtr) override;

private:
    /// Publish a single formatted message to JetStream
    void sendMessage(const String & message, ColumnPtr key_col, ColumnPtr headers_col, ColumnPtr ts_col);

    jsCtx * js_context;
    String subject;

    /// Column position tracking for _tp_message_key, _tp_message_headers, _tp_time
    std::optional<size_t> msg_key_column_pos;
    std::optional<size_t> headers_column_pos;
    std::optional<size_t> ts_column_pos;

    bool one_message_per_row{false};

    std::unique_ptr<MessageQueueFormatExecutor> format_executor;

    /// Row counter within current batch for per-row column extraction
    size_t current_batch_row{0};

    struct State
    {
        std::atomic_size_t outstanding{0};
        std::atomic_size_t acked{0};
        std::atomic_size_t error_count{0};
        std::atomic_int32_t last_error_code{0};

        void reset();
    };

    State state;

    /// the number of outstanding messages for the current checkpoint period
    size_t outstandingMessages() const noexcept { return state.outstanding - (state.acked + state.error_count); }

    UInt64 checkpoint_timeout_ms{0};
    std::atomic_flag is_finished{false};

    ExternalStreamCounterPtr external_stream_counter;
    LoggerPtr logger;
};

}

#endif
