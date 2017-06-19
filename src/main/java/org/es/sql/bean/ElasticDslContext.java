package org.es.sql.bean;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;

public class ElasticDslContext {
    //SQL
    private SQLQueryExpr queryExpr;
    //SQL Args
    private org.es.sql.bean.SQLArgs SQLArgs;
    //Result
    private ElasticSqlParseResult parseResult;

    public ElasticDslContext(SQLQueryExpr queryExpr, org.es.sql.bean.SQLArgs SQLArgs) {
        this.queryExpr = queryExpr;
        this.SQLArgs = SQLArgs;
        parseResult = new ElasticSqlParseResult();
    }

    public org.es.sql.bean.SQLArgs getSQLArgs() {
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
