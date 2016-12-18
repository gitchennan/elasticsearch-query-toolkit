package org.elasticsearch.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.Token;

public class ElasticSqlExprParser extends SQLExprParser {

    public ElasticSqlExprParser(Lexer lexer) {
        super(lexer);
    }

    public ElasticSqlExprParser(String sql) {
        this(new ElasticSqlLexer(sql));
        this.lexer.nextToken();
    }

    @Override
    public SQLSelectParser createSelectParser() {
        return new ElasticSqlSelectParser(this);
    }

    public ElasticSqlSelectQueryBlock.Limit parseLimit() {
        if (lexer.token() == Token.LIMIT) {
            lexer.nextToken();

            ElasticSqlSelectQueryBlock.Limit limit = new ElasticSqlSelectQueryBlock.Limit();

            SQLExpr temp = this.expr();
            if (lexer.token() == (Token.COMMA)) {
                limit.setOffset(temp);
                lexer.nextToken();
                limit.setRowCount(this.expr());
            } else if (identifierEquals("OFFSET")) {
                limit.setRowCount(temp);
                lexer.nextToken();
                limit.setOffset(this.expr());
            } else {
                limit.setRowCount(temp);
            }
            return limit;
        }

        return null;
    }
}
