#pragma once

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>

namespace DB
{
struct ASTEmitQuery;

/** Query like this:
  * EMIT STREAM AFTER WINDOW CLOSE WITH DELAY 1s AND TIMEOUT 5s
  * EMIT STREAM PERIODIC 1s REPEAT WITH DELAY 1s AND TIMEOUT 5s
  * EMIT ON UPDATE WITH DELAY 1s AND TIMEOUT 5s
  * EMIT ON UPDATE WITH BATCH 1s WITH DELAY 1s AND TIMEOUT 5s
  * EMIT LAST 1h ON PROCTIME
  */
class ParserEmitQuery : public IParserBase
{
public:
    explicit ParserEmitQuery(bool parse_only_internals_ = false) : parse_only_internals(parse_only_internals_) { }

private:
    const char * getName() const override { return "Emit query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint) override;
    bool extractSessionColumns(ASTPtr session_expr, std::shared_ptr<ASTEmitQuery> emit);

private:
    bool parse_only_internals;
};

}
