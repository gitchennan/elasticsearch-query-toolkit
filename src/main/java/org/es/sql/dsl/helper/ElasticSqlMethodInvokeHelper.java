package org.es.sql.dsl.helper;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.es.sql.dsl.enums.SortOption;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.parser.query.method.MethodInvocation;

import java.util.List;

public class ElasticSqlMethodInvokeHelper {
    public static final List<String> DATE_METHOD = ImmutableList.of("date", "to_date", "toDate");
    public static final List<String> NVL_METHOD = ImmutableList.of("nvl", "is_null", "isnull");
    public static final List<String> SCRIPT_SORT_METHOD = ImmutableList.of("script_sort", "scriptSort");

    public static final List<String> AGG_TERMS_METHOD = ImmutableList.of("terms", "terms_agg");
    public static final List<String> AGG_RANGE_METHOD = ImmutableList.of("range", "range_agg");
    public static final List<String> AGG_RANGE_SEGMENT_METHOD = ImmutableList.of("segment", "segment_agg");

    public static final String AGG_MIN_METHOD = "min";
    public static final String AGG_MAX_METHOD = "max";
    public static final String AGG_AVG_METHOD = "avg";
    public static final String AGG_SUM_METHOD = "sum";

    public static Boolean isMethodOf(List<String> methodAlias, String method) {
        if (CollectionUtils.isEmpty(methodAlias)) {
            return Boolean.FALSE;
        }
        for (String alias : methodAlias) {
            if (alias.equalsIgnoreCase(method)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    public static Boolean isMethodOf(String methodAlias, String method) {
        if (StringUtils.isBlank(methodAlias)) {
            return Boolean.FALSE;
        }
        return methodAlias.equalsIgnoreCase(method);
    }

    public static void checkScriptSortMethod(MethodInvocation scriptSortMethodInvocation) {
        // todo
    }

    public static void checkTermsAggMethod(SQLMethodInvokeExpr aggInvokeExpr) {
        if (!isMethodOf(AGG_TERMS_METHOD, aggInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] Sql not support method:" + aggInvokeExpr.getMethodName());
        }
    }

    public static void checkRangeAggMethod(SQLMethodInvokeExpr aggInvokeExpr) {
        if (!isMethodOf(AGG_RANGE_METHOD, aggInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] Sql not support method:" + aggInvokeExpr.getMethodName());
        }
    }

    public static void checkRangeItemAggMethod(SQLMethodInvokeExpr aggInvokeExpr) {
        if (!isMethodOf(AGG_RANGE_SEGMENT_METHOD, aggInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] Sql not support method:" + aggInvokeExpr.getMethodName());
        }
    }

    public static void checkStatAggMethod(SQLAggregateExpr statAggExpr) {
        if (!AGG_MIN_METHOD.equalsIgnoreCase(statAggExpr.getMethodName()) &&
                !AGG_MAX_METHOD.equalsIgnoreCase(statAggExpr.getMethodName()) &&
                !AGG_AVG_METHOD.equalsIgnoreCase(statAggExpr.getMethodName()) &&
                !AGG_SUM_METHOD.equalsIgnoreCase(statAggExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] Sql not support method:" + statAggExpr.getMethodName());
        }
    }

    public static void checkDateMethod(SQLMethodInvokeExpr dateInvokeExpr) {
        if (!isMethodOf(DATE_METHOD, dateInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] Sql not support method:" + dateInvokeExpr.getMethodName());
        }

        if (CollectionUtils.isEmpty(dateInvokeExpr.getParameters()) || dateInvokeExpr.getParameters().size() != 2) {
            throw new ElasticSql2DslException(String.format("[syntax error] There is no %s args method named date",
                    dateInvokeExpr.getParameters() != null ? dateInvokeExpr.getParameters().size() : 0));
        }

        SQLExpr patternArg = dateInvokeExpr.getParameters().get(0);
        SQLExpr timeValArg = dateInvokeExpr.getParameters().get(1);

        if (!(patternArg instanceof SQLCharExpr) && !(patternArg instanceof SQLVariantRefExpr)) {
            throw new ElasticSql2DslException("[syntax error] The first arg of date method should be a time pattern");
        }

        if (!(timeValArg instanceof SQLCharExpr) && !(timeValArg instanceof SQLVariantRefExpr)) {
            throw new ElasticSql2DslException("[syntax error] The second arg of date method should be a string of time");
        }
    }

    public static void checkNvlMethod(MethodInvocation nvlMethodInvocation) {
        if (!isMethodOf(NVL_METHOD, nvlMethodInvocation.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] Sql sort condition only support nvl method invoke");
        }

        int methodParameterCount = nvlMethodInvocation.getParameterCount();
        if (methodParameterCount == 0 || methodParameterCount > 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There is no %s args method named nvl", methodParameterCount));
        }

        SQLExpr fieldArg = nvlMethodInvocation.getParameter(0);
        SQLExpr valueArg = nvlMethodInvocation.getParameter(1);

        if (!(fieldArg instanceof SQLPropertyExpr) && !(fieldArg instanceof SQLIdentifierExpr)) {
            throw new ElasticSql2DslException("[syntax error] The first arg of nvl method should be field param name");
        }

        if (!(valueArg instanceof SQLCharExpr) && !(valueArg instanceof SQLIntegerExpr) && !(valueArg instanceof SQLNumberExpr)) {
            throw new ElasticSql2DslException("[syntax error] The second arg of nvl method should be number or string");
        }

        if (methodParameterCount == 3) {
            SQLExpr sortModArg = nvlMethodInvocation.getParameter(2);
            if (!(sortModArg instanceof SQLCharExpr)) {
                throw new ElasticSql2DslException("[syntax error] The third arg of nvl method should be string");
            }
            String sortModeText = ((SQLCharExpr) sortModArg).getText();
            if (!SortOption.AVG.mode().equalsIgnoreCase(sortModeText) && !SortOption.MIN.mode().equalsIgnoreCase(sortModeText)
                    && !SortOption.MAX.mode().equalsIgnoreCase(sortModeText) && !SortOption.SUM.mode().equalsIgnoreCase(sortModeText)) {
                throw new ElasticSql2DslException("[syntax error] The third arg of nvl method should be one of the string[min,max,avg,sum]");
            }
        }
    }
}
