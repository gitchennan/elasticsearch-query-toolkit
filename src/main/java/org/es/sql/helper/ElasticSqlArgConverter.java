package org.es.sql.helper;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import org.es.sql.bean.SQLArgs;
import org.es.sql.exception.ElasticSql2DslException;

import java.util.List;

public class ElasticSqlArgConverter {

    private ElasticSqlArgConverter() {

    }

    public static Object[] convertSqlArgs(List<SQLExpr> exprList, SQLArgs SQLArgs) {
        Object[] values = new Object[exprList.size()];
        for (int idx = 0; idx < exprList.size(); idx++) {
            values[idx] = convertSqlArg(exprList.get(idx), SQLArgs, true);
        }
        return values;
    }

    public static Object convertSqlArg(SQLExpr expr, SQLArgs SQLArgs) {
        return convertSqlArg(expr, SQLArgs, true);
    }

    public static Object convertSqlArg(SQLExpr expr, SQLArgs SQLArgs, boolean recognizeDateArg) {
        if (expr instanceof SQLVariantRefExpr) {
            SQLVariantRefExpr varRefExpr = (SQLVariantRefExpr) expr;
            //parse date
            if (recognizeDateArg && ElasticSqlDateParseHelper.isDateArgObjectValue(SQLArgs.get(varRefExpr.getIndex()))) {
                return ElasticSqlDateParseHelper.formatDefaultEsDateObjectValue(SQLArgs.get(varRefExpr.getIndex()));
            }
            return SQLArgs.get(varRefExpr.getIndex());
        }

        //numbers
        if (expr instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) expr).getNumber().longValue();
        }
        if (expr instanceof SQLNumberExpr) {
            return ((SQLNumberExpr) expr).getNumber().doubleValue();
        }

        //string
        if (expr instanceof SQLCharExpr) {
            Object textObject = ((SQLCharExpr) expr).getValue();
            //parse date
            if (recognizeDateArg && (textObject instanceof String) && ElasticSqlDateParseHelper.isDateArgStringValue((String) textObject)) {
                return ElasticSqlDateParseHelper.formatDefaultEsDateStringValue((String) textObject);
            }
            return textObject;
        }

        //method call
        if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodExpr = (SQLMethodInvokeExpr) expr;

            //parse date method
            if (ElasticSqlDateParseHelper.isDateMethod(methodExpr)) {
                ElasticSqlMethodInvokeHelper.checkDateMethod(methodExpr);
                String patternArg = (String) ElasticSqlArgConverter.convertSqlArg(methodExpr.getParameters().get(0), SQLArgs, false);
                String timeValArg = (String) ElasticSqlArgConverter.convertSqlArg(methodExpr.getParameters().get(1), SQLArgs, false);
                return ElasticSqlDateParseHelper.formatDefaultEsDate(patternArg, timeValArg);
            }
        }

        throw new ElasticSql2DslException(
                String.format("[syntax error] Arg type[%s] can not support.",
                        expr.toString()));
    }
}
