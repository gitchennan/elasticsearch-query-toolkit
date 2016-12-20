package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.google.common.collect.Lists;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;

import java.util.List;

public class ElasticSqlParseUtil {
    public static Object[] transferSqlArgs(List<SQLExpr> exprList) {
        SQLExpr firstArg = exprList.get(0);
        if (firstArg instanceof SQLIntegerExpr) {
            return toObjectArray(Lists.transform(exprList, argItem -> {
                return ((SQLIntegerExpr) argItem).getNumber().longValue();
            }));
        }
        if (firstArg instanceof SQLNumberExpr) {
            return toObjectArray(Lists.transform(exprList, argItem -> {
                return ((SQLNumberExpr) argItem).getNumber().doubleValue();
            }));
        }
        if (firstArg instanceof SQLCharExpr) {
            return toObjectArray(Lists.transform(exprList, argItem -> {
                return ((SQLCharExpr) argItem).getValue();
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
        throw new ElasticSql2DslException("[syntax error] Can not support arg type: " + expr.getClass());
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
