#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingCount.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingTime.h>

namespace DB::Streaming
{

void HybridAggregator::initStates(HybridAggregatedDataVariants & result) const
{
    if (method_chosen != HybridHashType::WithoutKey)
    {
        result.key_sizes = key_sizes;

        auto init_table = [&](HybridHashTableTemplate & table, std::string_view name, bool honor_ttl) {
            HybridHashTableConfig config;
            config.base_conf = result.getSubConfig(name, /*unshared=*/false);

            if (!honor_ttl)
                config.base_conf.ttl = 0;

            config.value_object_size = total_size_of_aggregate_states;
            config.align_value_object_size = align_aggregate_states;
            config.value_constructor = [this](void * data) { createAggregateStates(reinterpret_cast<AggregateDataPtr>(data)); };

            config.value_destructor = [this](void * data) { destroyAggregateStates(reinterpret_cast<AggregateDataPtr>(data)); };

            config.value_serializer = [this](const void * data, WriteBuffer & wb) {
                serializeAggregateStates(reinterpret_cast<ConstAggregateDataPtr>(data), wb);
                return ErrorCodes::OK;
            };

            config.value_deserializer = [this](void * data, ReadBuffer & rb) {
                deserializeAggregateStates(reinterpret_cast<AggregateDataPtr>(data), rb);
                return ErrorCodes::OK;
            };

            config.validate();
            table.init(method_chosen, std::move(config), result.key_sizes, logger, bucket_key_offset);
        };

        switch (params->tracking_updates_type)
        {
            case TrackingUpdatesType::UpdatesWithRetract:
            {
                init_table(result.table, "main", /*honor_ttl=*/true);
                /// Disable ttl for updates hybrid hash table
                init_table(result.retracts, "retracts", /*honor_ttl=*/false);
                break;
            }
            case TrackingUpdatesType::Updates:
            {
                init_table(result.table, "main", /*honor_ttl=*/true);
                
                HybridHashTableConfig config;
                config.base_conf = result.getSubConfig("changes", /*unshared=*/false);
                /// Disable ttl for updates hybrid hash table
                config.base_conf.ttl = 0;
                config.installNoOpCallbacks();
                config.validate();
                result.updates.init(method_chosen, std::move(config), result.key_sizes, logger, bucket_key_offset);
                break;
            }
            case TrackingUpdatesType::None:
            {
                init_table(result.table, "main", /*honor_ttl=*/true);
                break;
            }
        }

        if (params->emit_session_params)
        {
            auto list_config = result.getSubConfig("list", /*unshared=*/false);
            list_config.validate();

            /// Disable ttl for hybrid key list
            list_config.ttl = 0;
            result.outstanding_keys.init(method_chosen, std::move(list_config), result.key_sizes, logger);
        }
    }
    else
    {
        result.without_key_states_constructor = [this](AggregateDataPtr data) { createAggregateStates(data); };
        result.without_key_states_destructor = [this](AggregateDataPtr data) { destroyAggregateStates(data); };
        result.initWithoutKeyStates(total_size_of_aggregate_states, align_aggregate_states);
    }
}

void HybridAggregator::createAggregateStates(AggregateDataPtr aggregate_data) const
{
    if (trackingStateCount())
        TrackingCount::init(aggregate_data + tracking_count_offset);

    if (trackingStateTime())
        TrackingTime::init(aggregate_data + tracking_time_offset);

    for (size_t j = 0; j < params->aggregates_size; ++j)
    {
        try
        {
            /// An exception may occur if there is a shortage of memory.
            /// In order that then everything is properly destroyed, we "roll back" some of the created states.
            /// The code is not very convenient.
            aggregate_functions[j]->create(aggregate_data + offsets_of_aggregate_states[j]);
        }
        catch (...)
        {
            for (size_t rollback_j = 0; rollback_j < j; ++rollback_j)
                aggregate_functions[rollback_j]->destroy(aggregate_data + offsets_of_aggregate_states[rollback_j]);

            throw;
        }
    }
}

void HybridAggregator::destroyAggregateStates(AggregateDataPtr aggregate_data) const
{
    doDestroyAggregateStates(aggregate_data);
}

void HybridAggregator::serializeAggregateStates(ConstAggregateDataPtr place, DB::WriteBuffer & wb) const
{
    chassert(place);

    if (trackingStateCount()) /// Added in V3
        TrackingCount::serialize(place + tracking_count_offset, wb);

    if (trackingStateTime())
        TrackingTime::serialize(place + tracking_time_offset, wb);

    for (size_t i = 0; i < params->aggregates_size; ++i)
        aggregate_functions[i]->serialize(place + offsets_of_aggregate_states[i], wb);
}

void HybridAggregator::deserializeAggregateStates(
    AggregateDataPtr place, ReadBuffer & rb, VersionType version, std::optional<size_t> old_aggregates_size) const
{
    chassert(place);

    if (trackingStateCount())
    {
        if (version >= 3)
            TrackingCount::deserialize(place + tracking_count_offset, rb);
        else
            /// Always mark as non empty for compatibility, use the middle value of uint64,
            /// whether add() or negate(), ensure it is not empty
            TrackingCount::data(place + tracking_count_offset).count = std::numeric_limits<int64_t>::max();
    }

    if (trackingStateTime())
        TrackingTime::deserialize(place + tracking_time_offset, rb);

    auto aggregates_size = old_aggregates_size.value_or(params->aggregates_size);
    chassert(aggregates_size <= params->aggregates_size);
    for (size_t i = 0; i < aggregates_size; ++i)
        aggregate_functions[i]->deserialize(place + offsets_of_aggregate_states[i], rb, std::nullopt, /*arena=*/nullptr); /// FIXME, arena
}
}
