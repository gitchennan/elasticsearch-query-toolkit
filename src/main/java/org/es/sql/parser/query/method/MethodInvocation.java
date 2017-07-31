package org.es.sql.parser.query.method;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.es.sql.bean.SQLArgs;
import org.es.sql.helper.ElasticSqlArgConverter;


import java.util.List;

public class MethodInvocation {
    private final SQLMethodInvokeExpr methodInvokeExpr;
    private final String queryAs;
    private final SQLArgs sqlArgs;

    public MethodInvocation(SQLMethodInvokeExpr methodInvokeExpr, String queryAs, SQLArgs sqlArgs) {
        if (methodInvokeExpr == null) {
            throw new IllegalArgumentException("method invoke expression can not be null");
        }
        this.methodInvokeExpr = methodInvokeExpr;
        this.queryAs = queryAs;
        this.sqlArgs = sqlArgs;
    }

    public String getQueryAs() {
        return queryAs;
    }

    public SQLArgs getSqlArgs() {
        return sqlArgs;
    }

    public String getMethodName() {
        return methodInvokeExpr.getMethodName();
    }

    public List<SQLExpr> getParameters() {
        return methodInvokeExpr.getParameters();
    }

    public int getParameterCount() {
        return methodInvokeExpr.getParameters().size();
    }

    public SQLExpr getFirstParameter() {
        return getParameter(0);
    }

    public SQLExpr getParameter(int index) {
        return methodInvokeExpr.getParameters().get(index);
    }

    public Object getParameterAsObject(int index) {
        SQLExpr paramExpr = methodInvokeExpr.getParameters().get(index);
        return ElasticSqlArgConverter.convertSqlArg(paramExpr, sqlArgs, false);
    }

    public String getParameterAsFormatDate(int index) {
        SQLExpr paramExpr = methodInvokeExpr.getParameters().get(index);
        return ElasticSqlArgConverter.convertSqlArg(paramExpr, sqlArgs, true).toString();
    }

    public String getParameterAsString(int index) {
        return getParameterAsObject(index).toString();
    }

    public String getLastParameterAsString() {
        return getParameterAsObject(getParameterCount() - 1).toString();
    }

    public Double getParameterAsDouble(int index) {
        return (Double) getParameterAsObject(index);
    }

    public Long getParameterAsLong(int index) {
        return (Long) getParameterAsObject(index);
    }
}
