#pragma once

#include <Parsers/IAST.h>

namespace DB
{
struct ASTEmitQuery : public IAST
{
public:
    enum StreamMode
    {
        STREAM,
        CHANGELOG,
        DELTA,
    };
    std::optional<StreamMode> stream_mode;

    /// [AFTER WINDOW CLOSE]
    bool after_window_close = false;

    /// [WITH DELAY 1s]
    ASTPtr delay_interval;

    /// [PERIODIC 1s [REPEAT]]
    ASTPtr periodic_interval;
    bool repeat = false; /// If repeatedly emit last emitted results, no matter if there is new event or not

    /// [ON UPDATE [WITH BATCH 1s]]
    bool on_update = false;
    ASTPtr batch_interval;

    /// [PER EVENT]
    bool per_event = false;

    /// [TIMEOUT 5s]
    ASTPtr timeout_interval;

    /// [LAST <last-x> [ON PROCTIME]]]
    ASTPtr last_interval;
    bool proc_time = false; /// Proc time or event time processing.

    /// AFTER KEY EXPIRE [IDENTIFIED BY ts_col] WITH [ONLY] MAXSPAN <interval> AND TIMEOUT <interval>
    /// AFTER SESSION CLOSE [IDENTIFIED BY ts_col, session_start_bool_column, session_end_bool_column] WITH [ONLY] MAXSPAN <interval> AND TIMEOUT <interval>
    ASTPtr session_ts_col;
    ASTPtr session_start_col;
    ASTPtr session_end_col;
    ASTPtr session_max_span_interval;
    /// Only emit session keys whose span >= max span
    bool only_max_span_session = false;

    String getID(char) const override { return "Emit"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

    void updateTreeHashImpl(SipHash & hash_state) const override;
};
}
