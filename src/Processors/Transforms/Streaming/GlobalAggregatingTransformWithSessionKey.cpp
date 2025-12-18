#include <Processors/Transforms/Streaming/GlobalAggregatingTransformWithSessionKey.h>

namespace DB::Streaming
{
GlobalAggregatingTransformWithSessionKey::GlobalAggregatingTransformWithSessionKey(
    Block header, AggregatingTransformParamsPtr params_, const std::string & id)
    : AggregatingTransform(
          std::move(header),
          params_,
          std::make_unique<ManyAggregatedData>(params_->aggregatorType(), id),
          0,
          1,
          "GlobalAggregatingTransformWithSessionKey",
          ProcessorID::GlobalAggregatingTransformWithSessionKeyID)
{
    chassert(params->params->group_by == IAggregatorParams::GroupBy::Other);
    chassert(params->aggregatorType() == AggregatorType::Hybrid);
}

String GlobalAggregatingTransformWithSessionKey::getName() const
{
    return "HybridGlobalAggregatingTransformWithSessionKey";
}

}
