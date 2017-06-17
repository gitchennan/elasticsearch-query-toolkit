package org.es.sql.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.Token;

public class ElasticSqlSelectParser extends SQLSelectParser {

    public ElasticSqlSelectParser(SQLExprParser exprParser) {
        super(exprParser);
    }

    public ElasticSqlSelectQueryBlock.Limit parseLimit() {
        return ((ElasticSqlExprParser) this.exprParser).parseLimit();
    }

    public ElasticSqlSelectQueryBlock.Routing parseRoutingBy() {
        return ((ElasticSqlExprParser) this.exprParser).parseRoutingBy();
    }

    @Override
    public SQLSelectQuery query() {
        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            SQLSelectQuery select = query();
            accept(Token.RPAREN);
            return queryRest(select);
        }

        accept(Token.SELECT);

        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
        }

        ElasticSqlSelectQueryBlock queryBlock = new ElasticSqlSelectQueryBlock();

        if (lexer.token() == Token.DISTINCT) {
            queryBlock.setDistionOption(SQLSetQuantifier.DISTINCT);
            lexer.nextToken();
        }
        else if (lexer.token() == Token.UNIQUE) {
            queryBlock.setDistionOption(SQLSetQuantifier.UNIQUE);
            lexer.nextToken();
        }
        else if (lexer.token() == Token.ALL) {
            queryBlock.setDistionOption(SQLSetQuantifier.ALL);
            lexer.nextToken();
        }

        parseSelectList(queryBlock);
        parseFrom(queryBlock);
        parseMatchQuery(queryBlock);
        parseWhere(queryBlock);
        parseGroupBy(queryBlock);
        queryBlock.setOrderBy(this.exprParser.parseOrderBy());

        if (lexer.token() == Token.INDEX && "ROUTING".equalsIgnoreCase(lexer.stringVal())) {
            queryBlock.setRouting(parseRoutingBy());
        }

        if (lexer.token() == Token.LIMIT) {
            queryBlock.setLimit(parseLimit());
        }

        return queryRest(queryBlock);
    }

    @Override
    public SQLTableSource parseTableSource() {
        if (lexer.token() != Token.IDENTIFIER) {
            throw new ParserException(
                    "[syntax error] from table source is not a identifier");
        }

        SQLExprTableSource tableReference = new SQLExprTableSource();
        parseTableSourceQueryTableExpr(tableReference);
        SQLTableSource tableSrc = parseTableSourceRest(tableReference);
        if (lexer.hasComment() && lexer.isKeepComments()) {
            tableSrc.addAfterComment(lexer.readAndResetComments());
        }

        return tableSrc;
    }

    protected void parseMatchQuery(ElasticSqlSelectQueryBlock queryBlock) {
        if (lexer.token() == Token.INDEX && "QUERY".equalsIgnoreCase(lexer.stringVal())) {
            lexer.nextToken();

            SQLExpr matchQuery = expr();

            queryBlock.setMatchQuery(matchQuery);
        }
    }
}
