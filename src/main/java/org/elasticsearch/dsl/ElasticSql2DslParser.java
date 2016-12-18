package org.elasticsearch.dsl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.parser.Token;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.sql.ElasticSqlExprParser;

public class ElasticSql2DslParser {

    private String sql;

    public ElasticSql2DslParser(String sql) {
        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("constructor args [sql] cannot be blank!");
        }
        this.sql = sql;
    }

    /**
     * 执行sql到dsl转换
     *
     * @return 解析后dsl上下文
     * @throws ElasticSql2DslException 当解析出现语法错误时抛出
     */
    public ElasticDslContext parse() {
        ElasticSqlExprParser elasticSqlExprParser = new ElasticSqlExprParser(sql);
        SQLExpr sqlQueryExpr = elasticSqlExprParser.expr();

        check(elasticSqlExprParser, sqlQueryExpr);

        ElasticDslBuilder dslBuilder = new ElasticDslBuilder((SQLQueryExpr) sqlQueryExpr);
        return dslBuilder.build();
    }

    private void check(ElasticSqlExprParser sqlExprParser, SQLExpr sqlQueryExpr) {
        if (sqlExprParser.getLexer().token() != Token.EOF) {
            throw new ElasticSql2DslException("Sql last token is not EOF");
        }

        if (!(sqlQueryExpr instanceof SQLQueryExpr)) {
            throw new ElasticSql2DslException("Sql is not select sql");
        }
    }

}
