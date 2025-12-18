#pragma once

#include <Core/Streaming/Watermark.h>
#include <Interpreters/Streaming/WindowCommon.h>

namespace DB
{
struct TreeRewriterResult;
using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

namespace Streaming
{

struct EmitParams
{
public:
    EmitParams(ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, WindowParamsPtr window_params_);

    std::pair<UInt64, UInt64> keyMaxSpanAndTimeoutMs() const;

    WindowParamsPtr window_params;

    EmitMode mode = EmitMode::None;

    WindowInterval periodic_interval;

    /// With timeout
    WindowInterval timeout_interval;

    /// With delay
    WindowInterval delay_interval;

    /// EMIT AFTER SESSION CLOSE
    WindowInterval session_max_span_interval;
    String session_ts_col_name;
    String session_start_col_name;
    String session_end_col_name;
    bool only_max_span_session = false;

    bool repeat = false;
    bool delta = false;
};

using EmitParamsPtr = std::shared_ptr<const EmitParams>;

}

}
