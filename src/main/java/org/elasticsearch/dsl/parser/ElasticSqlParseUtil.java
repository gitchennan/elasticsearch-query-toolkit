package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class ElasticSqlParseUtil {
    public static Object[] transferSqlArgs(List<SQLExpr> exprList) {
        SQLExpr firstArg = exprList.get(0);
        if (firstArg instanceof SQLIntegerExpr) {
            return toObjectArray(Lists.transform(exprList, new Function<SQLExpr, Object>() {
                @Override
                public Object apply(SQLExpr sqlExpr) {
                    return ((SQLIntegerExpr) sqlExpr).getNumber().longValue();
                }
            }));
        }
        if (firstArg instanceof SQLNumberExpr) {
            return toObjectArray(Lists.transform(exprList, new Function<SQLExpr, Object>() {
                @Override
                public Object apply(SQLExpr sqlExpr) {
                    return ((SQLNumberExpr) sqlExpr).getNumber().doubleValue();
                }
            }));
        }
        if (firstArg instanceof SQLCharExpr) {
            return toObjectArray(Lists.transform(exprList, new Function<SQLExpr, Object>() {
                @Override
                public Object apply(SQLExpr sqlExpr) {
                    return ((SQLCharExpr) sqlExpr).getValue();
                }
            }));
        }
        throw new ElasticSql2DslException("[syntax error] Can not support arg type: " + firstArg.getClass());
    }

    public static Object transferSqlArg(SQLExpr expr) {
        if (expr instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) expr).getNumber().longValue();
        }
        if (expr instanceof SQLNumberExpr) {
            return ((SQLNumberExpr) expr).getNumber().doubleValue();
        }
        if (expr instanceof SQLCharExpr) {
            return ((SQLCharExpr) expr).getValue();
        }
        if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr dateMethodExpr = (SQLMethodInvokeExpr) expr;
            checkDateMethod(dateMethodExpr);
            final String patternArg = (String) ElasticSqlParseUtil.transferSqlArg(dateMethodExpr.getParameters().get(0));
            final String timeValArg = (String) ElasticSqlParseUtil.transferSqlArg(dateMethodExpr.getParameters().get(1));
            try {
                SimpleDateFormat dateFormat = new SimpleDateFormat(patternArg);
                Date date = dateFormat.parse(timeValArg);

                dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                return dateFormat.format(date);
            } catch (ParseException pex) {
                throw new ElasticSql2DslException("[syntax error] Parse time arg error: " + expr.toString());
            }
        }
        throw new ElasticSql2DslException("[syntax error] Can not support arg type: " + expr.toString());
    }

    private static void checkDateMethod(SQLMethodInvokeExpr dateInvokeExpr) {
        if (!"date".equalsIgnoreCase(dateInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] ElasticSql not support method:" + dateInvokeExpr.getMethodName());
        }

        if (CollectionUtils.isEmpty(dateInvokeExpr.getParameters()) || dateInvokeExpr.getParameters().size() != 2) {
            throw new ElasticSql2DslException(String.format("[syntax error] There is no %s args method named nvl",
                    dateInvokeExpr.getParameters() != null ? dateInvokeExpr.getParameters().size() : 0));
        }

        SQLExpr patternArg = dateInvokeExpr.getParameters().get(0);
        SQLExpr timeValArg = dateInvokeExpr.getParameters().get(1);

        if (!(patternArg instanceof SQLCharExpr)) {
            throw new ElasticSql2DslException("[syntax error] The first arg of date method should be a time pattern");
        }

        if (!(timeValArg instanceof SQLCharExpr)) {
            throw new ElasticSql2DslException("[syntax error] The second arg of date method should be a string of time");
        }
    }

    private static <T> Object[] toObjectArray(List<T> list) {
        return list.toArray(new Object[list.size()]);
    }

    public static boolean isValidBinOperator(SQLBinaryOperator binaryOperator) {
        return binaryOperator == SQLBinaryOperator.Equality
                || binaryOperator == SQLBinaryOperator.NotEqual
                || binaryOperator == SQLBinaryOperator.LessThanOrGreater
                || binaryOperator == SQLBinaryOperator.GreaterThan
                || binaryOperator == SQLBinaryOperator.GreaterThanOrEqual
                || binaryOperator == SQLBinaryOperator.LessThan
                || binaryOperator == SQLBinaryOperator.LessThanOrEqual
                || binaryOperator == SQLBinaryOperator.LessThanOrGreater
                || binaryOperator == SQLBinaryOperator.Is
                || binaryOperator == SQLBinaryOperator.IsNot;
    }
}
