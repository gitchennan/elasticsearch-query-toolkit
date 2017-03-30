package org.elasticsearch.dsl.bean;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;

public class ElasticDslContext {
    //SQL
    private SQLQueryExpr queryExpr;
    //SQL Args
    private Object[] sqlArgs;
    //Result
    private ElasticSqlParseResult parseResult;

    public ElasticDslContext(SQLQueryExpr queryExpr, Object[] sqlArgs) {
        this.queryExpr = queryExpr;
        this.sqlArgs = sqlArgs;
        parseResult = new ElasticSqlParseResult();
    }

    public Object[] getSqlArgs() {
        return sqlArgs;
    }

    public SQLQueryExpr getQueryExpr() {
        return queryExpr;
    }

    public ElasticSqlParseResult getParseResult() {
        return parseResult;
    }

    @Override
    public String toString() {
        return parseResult.toDsl();
    }
}
