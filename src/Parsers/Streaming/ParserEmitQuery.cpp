#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Parsers/Streaming/ParserEmitQuery.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int NOT_IMPLEMENTED;
}

bool ParserEmitQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    /// EMIT [STREAM|CHANGELOG|DELTA]
    ///     - AFTER WINDOW CLOSE                <=>  (Old) `AFTER WATERMARK`
    ///         - WITH DELAY <interval>
    ///         - AND TIMEOUT <interval>
    ///     - PERIODIC <interval> [REPEAT]
    ///         - WITH DELAY <interval>
    ///         - AND TIMEOUT <interval>
    ///     - ON UPDATE
    ///         - WITH DELAY <interval>
    ///         - AND TIMEOUT <interval>
    ///     - ON UPDATE WITH BATCH <interval>   <=>  (Old) `PERIODIC <interval> ON UPDATE`
    ///         - WITH DELAY <interval>
    ///         - AND TIMEOUT <interval>
    ///     - PER EVENT
    ///         - WITH DELAY <interval>
    ///         - AND TIMEOUT <interval>
    ///     - PER KEY IDENTIFIED BY ts_col WITH MAXSPAN <interval> AND TIMEOUT <interval>
    ///
    /// (Will be deprecated)
    /// EMIT LAST <last-x> [ON PROCTIME]]
    ///
    /// For example:
    /// 1) EMIT STREAM AFTER WINDOW CLOSE WITH DELAY 1s AND TIMEOUT 5s
    /// 2) EMIT STREAM PERIODIC 1s REPEAT WITH DELAY 1s AND TIMEOUT 5s
    /// 3) EMIT ON UPDATE WITH DELAY 1s AND TIMEOUT 5s
    /// 4) EMIT ON UPDATE WITH BATCH 1s WITH DELAY 1s AND TIMEOUT 5s
    /// 5) EMIT PER EVENT WITH DELAY 1s AND TIMEOUT 5s
    /// 5) EMIT LAST 1h ON PROCTIME
    /// ...
    if (!parse_only_internals)
    {
        ParserKeyword s_emit("EMIT");
        if (!s_emit.ignore(pos, expected))
            return false;
    }

    auto query = std::make_shared<ASTEmitQuery>();
    ParserIntervalOperatorExpression interval_alias_p;

    if (ParserKeyword("STREAM").ignore(pos, expected))
        query->stream_mode = ASTEmitQuery::StreamMode::STREAM;
    else if (ParserKeyword("CHANGELOG").ignore(pos, expected))
        query->stream_mode = ASTEmitQuery::StreamMode::CHANGELOG;
    else if (ParserKeyword("DELTA").ignore(pos, expected))
        query->stream_mode = ASTEmitQuery::StreamMode::DELTA;

    /// Special case : EMIT [STREAM] LAST <last-x> [ON PROCTIME]]
    if (ParserKeyword("LAST").ignore(pos, expected))
    {
        if (query->stream_mode.value_or(ASTEmitQuery::StreamMode::STREAM) != ASTEmitQuery::StreamMode::STREAM)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "EMIT LAST clause only supports `STREAM` EMIT");

        if (!interval_alias_p.parse(pos, query->last_interval, expected))
            return false;

        if (ParserKeyword("ON").ignore(pos, expected))
        {
            if (ParserKeyword("PROCTIME").ignore(pos, expected))
                query->proc_time = true;
            else
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Expect 'PROCTIME' after 'ON' in EMIT LAST clause");
        }
        node = std::move(query);
        return true;
    }

    /// EMIT [STREAM] AFTER KEY EXPIRE [IDENTIFIED BY ts_col] WITH [ONLY] MAXSPAN <interval> [AND TIMEOUT <interval>]
    /// EMIT [STREAM] AFTER SESSION CLOSE [IDENTIFIED BY (ts_col, session_start_col, session_end_col)] WITH [ONLY] MAXSPAN <interval> [AND TIMEOUT <interval>]
    if (ParserKeyword("AFTER SESSION CLOSE").ignore(pos, expected) || ParserKeyword("AFTER KEY EXPIRE").ignore(pos, expected))
    {
        if (ParserKeyword("IDENTIFIED BY").ignore(pos, expected))
        {
            ParserExpression session_expr;
            ASTPtr session_node = nullptr;
            if (!session_expr.parse(pos, session_node, expected))
                return false;

            if (!extractSessionColumns(session_node, query))
                return false;
        }

        if (!ParserKeyword("WITH MAXSPAN").ignore(pos, expected))
        {
            if (ParserKeyword("WITH ONLY MAXSPAN").ignore(pos, expected))
                query->only_max_span_session = true;
            else
                return false;
        }

        if (!interval_alias_p.parse(pos, query->session_max_span_interval, expected))
            return false;

        /// [AND TIMEOUT INTERVAL '5' SECONDS]
        if (ParserKeyword("AND TIMEOUT").ignore(pos, expected))
        {
            if (!interval_alias_p.parse(pos, query->timeout_interval, expected))
                return false;
        }

        node = std::move(query);
        return true;
    }

    auto parse_watermark_modifier = [&]() {
        /// [WITH DELAY INTERVAL '3' SECONDS]
        if (ParserKeyword("WITH DELAY").ignore(pos, expected))
        {
            if (!interval_alias_p.parse(pos, query->delay_interval, expected))
                return false;
        }

        /// [AND TIMEOUT INTERVAL '5' SECONDS]
        if (ParserKeyword("AND TIMEOUT").ignore(pos, expected))
        {
            if (!interval_alias_p.parse(pos, query->timeout_interval, expected))
                return false;
        }

        return true;
    };

    /// [AFTER WINDOW CLOSE]
    if (ParserKeyword("AFTER WINDOW CLOSE").ignore(pos, expected) || ParserKeyword("AFTER WATERMARK").ignore(pos, expected))
    {
        query->after_window_close = true;

        if (!parse_watermark_modifier())
            return false;
    }
    /// [PERIODIC <interval> [REPEAT]]
    else if (ParserKeyword("PERIODIC").ignore(pos, expected))
    {
        /// [PERIODIC INTERVAL '3' SECONDS]
        if (!interval_alias_p.parse(pos, query->periodic_interval, expected))
            return false;

        if (ParserKeyword("REPEAT").ignore(pos, expected))
        {
            query->repeat = true;
        }
        /// `PERIODIC <interval> ON UPDATE` is equal to `ON UPDATE WITH BATCH <interval>`
        else if (ParserKeyword("ON UPDATE").ignore(pos, expected))
        {
            query->batch_interval.swap(query->periodic_interval);
            query->on_update = true;
        }

        if (!parse_watermark_modifier())
            return false;
    }
    /// [ON UPDATE]
    else if (ParserKeyword("ON UPDATE").ignore(pos, expected))
    {
        query->on_update = true;

        // [WITH BATCH INTERVAL '3' SECONDS]
        if (ParserKeyword("WITH BATCH").ignore(pos, expected))
        {
            if (!interval_alias_p.parse(pos, query->batch_interval, expected))
                return false;
        }

        if (!parse_watermark_modifier())
            return false;
    }
    /// [PER EVENT]
    else if (ParserKeyword("PER EVENT").ignore(pos, expected))
    {
        query->per_event = true;

        if (!parse_watermark_modifier())
            return false;
    }
    else if (ParserKeyword("TIMEOUT").ignore(pos, expected))
    {
        /// For backward compatibility
        /// When `EMIT TIMEOUT 30s;` get applied to window aggregation or global aggregation
        /// `EMIT AFTER WINDOW CLOSE` or `EMIT PERIODIC` is implicitly implied
        if (!interval_alias_p.parse(pos, query->timeout_interval, expected))
            return false;
    }

    node = std::move(query);
    return true;
}

bool ParserEmitQuery::extractSessionColumns(ASTPtr session_expr, std::shared_ptr<ASTEmitQuery> emit)
{
    if (!session_expr)
        return false;

    /// There are 2 cases:
    /// 1) IDENTIFIED BY ts_col
    /// 2) IDENTIFIED BY (ts_col, start_col, end_col)
    ///    a) (ts_col, start_col, end_col)
    ///    b) (ts_col, start_col, false)
    ///    c) (ts_col, true, end_col)
    ///    d) (ts_col, true, false) => same as 1)
    const auto * expr_func = session_expr->as<ASTFunction>();
    if (expr_func && expr_func->name == "tuple_cast")
    {
        if (!expr_func->arguments)
            /// Empty tuple, expect at least 1
            return false;

        if (expr_func->arguments->children.size() == 3)
        {
            if (const auto * ts_col = expr_func->arguments->children[0]->as<ASTIdentifier>(); !ts_col)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "The first element in `IDENTIFIED BY` tuple is expected be a column identifier");

            emit->session_ts_col = expr_func->arguments->children[0];

            if (const auto * start_col = expr_func->arguments->children[1]->as<ASTIdentifier>(); start_col)
            {
                /// a) or b)
                emit->session_start_col = expr_func->arguments->children[1];
            }
            else if (const auto * start_col_lit = expr_func->arguments->children[1]->as<ASTLiteral>(); start_col_lit)
            {
                /// c) or d), it has to be true
                bool is_true = false;
                if (!start_col_lit->value.tryGet(is_true) || !is_true)
                    throw Exception(
                        ErrorCodes::SYNTAX_ERROR,
                        "The second element in `IDENTIFIED BY` tuple is expected be a column identifier or `true` literal");
            }
            else
            {
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR,
                    "The second element in `IDENTIFIED BY` tuple is expected be a column identifier or `true` literal");
            }

            if (const auto * end_col = expr_func->arguments->children[2]->as<ASTIdentifier>(); end_col)
            {
                /// a) or c)
                emit->session_end_col = expr_func->arguments->children[2];
            }
            else if (const auto * end_col_lit = expr_func->arguments->children[2]->as<ASTLiteral>(); end_col_lit)
            {
                /// b) or d), it has to be false
                bool is_true = false;
                if (!end_col_lit->value.tryGet(is_true) || is_true)
                    throw Exception(
                        ErrorCodes::SYNTAX_ERROR,
                        "The third element in `IDENTIFIED BY` tuple is expected be a column identifier or `false` literal");
            }
            else
            {
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR,
                    "The third element in `IDENTIFIED BY` tuple is expected be a column identifier or `false` literal");
            }
        }
        else if (expr_func->arguments->children.size() == 1)
        {
            if (const auto * ts_col = expr_func->arguments->children.back()->as<ASTIdentifier>(); !ts_col)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "The first element in `IDENTIFIED BY` tuple is expected be a column identifier");

            emit->session_ts_col = expr_func->arguments->children.back();
        }
        else
        {
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "`IDENTIFIED BY` tuple is expected to have 1 or 3 columns in `EMIT AFTER SESSION CLOSE`, got {} elements",
                expr_func->arguments->children.size());
        }
    }
    else
    {
        /// 1) IDENTIFIED BY ts_col, expected it is an identifier
        if (const auto * ts_col = session_expr->as<ASTIdentifier>(); !ts_col)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "The first element in `IDENTIFIED BY` tuple is expected be a column identifier");

        emit->session_ts_col = session_expr;
    }

    return true;
}

}
