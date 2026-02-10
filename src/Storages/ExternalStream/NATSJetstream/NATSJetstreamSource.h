#pragma once

#include "config.h"

#if USE_NATS

#include <Processors/Streaming/ISource.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/ExternalStreamSource.h>
#include <Common/Stopwatch.h>

#include <nats/nats.h>

namespace DB::ExternalStream
{

class NATSJetstream;

/// Streaming source for NATS JetStream external streams.
///
/// Implements pull-based consumption with durable subscription management.
/// Features:
/// - Configurable batch fetching and timeouts
/// - Automatic stall detection and subscription recreation
/// - Virtual column extraction (_tp_message_key, _tp_message_headers, timestamps)
/// - Sequence number tracking for checkpoint/recovery
/// - At-least-once delivery semantics via explicit ACKs
///
class NATSJetstreamSource final : public Streaming::ISource, public ExternalStreamSource
{
public:
    NATSJetstreamSource(
        const Block & header_,
        const StorageSnapshotPtr & storage_snapshot_,
        bool is_streaming_,
        const String & data_format,
        const FormatSettings & format_settings,
        natsSubscription * subscription_,
        std::shared_ptr<NATSJetstream> storage_,
        size_t max_block_size_,
        UInt64 stall_timeout_ms,
        ExternalStreamCounterPtr counter,
        LoggerPtr logger_,
        const ContextPtr & context_);

    ~NATSJetstreamSource() override;

    String getName() const override { return "NATSJetstreamSource"; }

    /// Reports the estimated [low, high] sequence range in the stream
    std::pair<Int64, Int64> sequenceRange() const override;

    Chunk generate() override;

protected:
    void onCancel() noexcept override;

private:
    /// Stall detection: recreates subscription if no progress for timeout_ms.
    /// Mirrors KafkaSource::StallDetector exactly.
    class StallDetector
    {
    public:
        StallDetector(UInt64 timeout_ms_, LoggerPtr logger_);
        /// Check if the source is stalled. If so, recreate.
        void checkAndHandleStall(NATSJetstreamSource & source);
        void onProgress();
        void reset();

    private:
        UInt64 timeout_ms{0};
        Int64 recorded_last_processed_sn{-1};
        Stopwatch timer;
        Stopwatch caught_up_timer;
        LoggerPtr logger;
    };

    /// Parse message format — mirrors KafkaSource::parseFormat
    void parseFormat(natsMsg * msg);

    /// Checkpoint: serialize (subject, consumer_name, last_processed_sn) to coordinator
    Chunk doCheckpoint(CheckpointContextPtr ckpt_ctx_) override;
    /// Recovery: read back serialized state; durable consumers resume automatically
    void doRecover(CheckpointContextPtr ckpt_ctx_) override;
    /// SN reset for auto-recovery
    void doResetStartSN(Int64 sn) override;

    /// Re-fetch raw data for a sequence range (audit/replay support)
    Strings doFetchData(const Streaming::SequenceRange &) override;

    void getPhysicalHeader() override;

    /// Virtual column extraction functions — per position in the header.
    /// nullptr means physical column. Matches Kafka's exact pattern.
    std::vector<std::function<Field(natsMsg *)>> virtual_col_value_functions;
    std::vector<DataTypePtr> virtual_col_types;
    bool request_virtual_columns = false;

    /// Generate timeout
    Int32 record_consume_timeout_ms{100};

    std::shared_ptr<NATSJetstream> storage;
    natsSubscription * subscription = nullptr;

    /// Tracking for checkpoint/recovery
    String current_subject;
    String current_consumer_name;
    Int64 messages_to_skip{0};

    bool ignore_format_errors = false;
    std::optional<Exception> consume_exception;

    /// Batch state — mirrors Kafka's result_chunks_with_sns
    MutableColumns current_batch;

    /// Lifecycle flags
    std::atomic_flag start_consume_flag;
    bool reached_the_end{false};

    StallDetector stall_detector;

    ExternalStreamCounterPtr external_stream_counter;
};

}

#endif
