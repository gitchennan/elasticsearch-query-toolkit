package org.elasticsearch.dsl.parser.query.method;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

public class MethodInvocation {
    private SQLMethodInvokeExpr matchQueryExpr;
    private String queryAs;
    private Object[] sqlArgs;

    public MethodInvocation(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs) {
        this.matchQueryExpr = matchQueryExpr;
        this.queryAs = queryAs;
        this.sqlArgs = sqlArgs;
    }

    public SQLMethodInvokeExpr getMatchQueryExpr() {
        return matchQueryExpr;
    }

    public String getQueryAs() {
        return queryAs;
    }

    public Object[] getSqlArgs() {
        return sqlArgs;
    }
}
