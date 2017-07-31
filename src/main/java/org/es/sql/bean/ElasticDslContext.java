package org.es.sql.bean;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;

public class ElasticDslContext {
    //SQL
    private SQLQueryExpr queryExpr;
    //SQL Args
    private SQLArgs SQLArgs;
    //Result
    private ElasticSqlParseResult parseResult;

    public ElasticDslContext(SQLQueryExpr queryExpr, SQLArgs SQLArgs) {
        this.queryExpr = queryExpr;
        this.SQLArgs = SQLArgs;
        parseResult = new ElasticSqlParseResult();
    }

    public ElasticDslContext(SQLQueryExpr queryExpr) {
        this.queryExpr = queryExpr;
        parseResult = new ElasticSqlParseResult();
    }

    public SQLArgs getSQLArgs() {
        return SQLArgs;
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
