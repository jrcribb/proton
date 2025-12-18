
#include <Parsers/Streaming/ASTSessionBoundary.h>
#include <Parsers/Streaming/ParserSessionBoundaryExpression.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
/// Parse range between predications that include/exclude boundary
/// [<start_cond>, <end_cond>]
/// [<start_cond>, <end_cond>)
/// (<start_cond>, <end_cond>]
/// (<start_cond>, <end_cond>)
bool ParserSessionBoundaryExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    Pos begin = pos;
    do
    {
        auto boundary = std::make_shared<ASTSessionBoundary>();
        /// Opening
        if (pos->type == TokenType::OpeningSquareBracket)
            boundary->start_with_inclusion = true;
        else if (pos->type == TokenType::OpeningRoundBracket)
            boundary->start_with_inclusion = false;
        else
            break;

        ++pos;

        /// Parsing <start predication, end predication>
        ASTPtr conds;
        if (!ParserList(std::make_unique<ParserExpression>(), std::make_unique<ParserToken>(TokenType::Comma))
                 .parse(pos, conds, expected, hint))
            break;

        auto & cond_list = conds->as<ASTExpressionList &>();
        if (cond_list.children.size() != 2)
            break;

        boundary->children = std::move(cond_list.children);

        /// Closing
        if (pos->type == TokenType::ClosingSquareBracket)
            boundary->end_with_inclusion = true;
        else if (pos->type == TokenType::ClosingRoundBracket)
            boundary->end_with_inclusion = false;
        else
            break;

        ++pos;
        node = std::move(boundary);
        return true;
    } while (false);

    pos = begin;
    return elem_parser->parse(pos, node, expected, hint);
};
}
