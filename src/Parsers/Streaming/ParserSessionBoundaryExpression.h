#pragma once

#include <Parsers/ExpressionElementParsers.h>

namespace DB
{
/// Parse range between predications that include/exclude boundary for session
/// [<start_cond>, <end_cond>]
/// [<start_cond>, <end_cond>)
/// (<start_cond>, <end_cond>]
/// (<start_cond>, <end_cond>)
class ParserSessionBoundaryExpression : public IParserBase
{
private:
    ParserPtr elem_parser;

public:
    explicit ParserSessionBoundaryExpression(ParserPtr && elem_parser_) : elem_parser(std::move(elem_parser_)) {}

protected:
    const char * getName() const override { return "session boundary expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};
}
