package org.elasticsearch.dsl.parser.helper;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.QueryOrderConditionParser;

public class ElasticSqlMethodInvokeHelper {

    public static final String DATE_METHOD = "date";
    public static final String NVL_METHOD = "nvl";
    public static final String INNER_DOC_METHOD = "inner_doc";
    public static final String NESTED_DOC_METHOD = "nested_doc";

    public static void checkDateMethod(SQLMethodInvokeExpr dateInvokeExpr) {
        if (!DATE_METHOD.equalsIgnoreCase(dateInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] ElasticSql not support method:" + dateInvokeExpr.getMethodName());
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

    public static void checkInnerDocMethod(SQLMethodInvokeExpr innerDocInvokeExpr) {
        if (!INNER_DOC_METHOD.equalsIgnoreCase(innerDocInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] ElasticSql not support method:" + innerDocInvokeExpr.getMethodName());
        }

        if (CollectionUtils.isEmpty(innerDocInvokeExpr.getParameters()) || innerDocInvokeExpr.getParameters().size() != 1) {
            throw new ElasticSql2DslException(String.format("[syntax error] There is no %s args method named inner_doc",
                    innerDocInvokeExpr.getParameters() != null ? innerDocInvokeExpr.getParameters().size() : 0));
        }

        SQLExpr innerPropertyName = innerDocInvokeExpr.getParameters().get(0);

        if (!(innerPropertyName instanceof SQLPropertyExpr) && !(innerPropertyName instanceof SQLIdentifierExpr)) {
            throw new ElasticSql2DslException("[syntax error] The arg of inner_doc method should be field param name");
        }
    }

    public static void checkNestedDocMethod(SQLMethodInvokeExpr nestedDocInvokeExpr) {
        if (!NESTED_DOC_METHOD.equalsIgnoreCase(nestedDocInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] ElasticSql not support method:" + nestedDocInvokeExpr.getMethodName());
        }

        if (CollectionUtils.isEmpty(nestedDocInvokeExpr.getParameters()) || nestedDocInvokeExpr.getParameters().size() != 1) {
            throw new ElasticSql2DslException(String.format("[syntax error] There is no %s args method named nested_doc",
                    nestedDocInvokeExpr.getParameters() != null ? nestedDocInvokeExpr.getParameters().size() : 0));
        }

        SQLExpr innerPropertyName = nestedDocInvokeExpr.getParameters().get(0);

        if (!(innerPropertyName instanceof SQLPropertyExpr) && !(innerPropertyName instanceof SQLIdentifierExpr)) {
            throw new ElasticSql2DslException("[syntax error] The arg of nested_doc method should be field param name");
        }
    }

    public static void checkNvlMethod(SQLMethodInvokeExpr nvlInvokeExpr) {
        if (!NVL_METHOD.equalsIgnoreCase(nvlInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] ElasticSql sort condition only support nvl method invoke");
        }

        if (CollectionUtils.isEmpty(nvlInvokeExpr.getParameters()) || nvlInvokeExpr.getParameters().size() > 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There is no %s args method named nvl",
                    nvlInvokeExpr.getParameters() != null ? nvlInvokeExpr.getParameters().size() : 0));
        }

        SQLExpr fieldArg = nvlInvokeExpr.getParameters().get(0);
        SQLExpr valueArg = nvlInvokeExpr.getParameters().get(1);

        if (fieldArg instanceof SQLMethodInvokeExpr) {
            if (INNER_DOC_METHOD.equalsIgnoreCase(((SQLMethodInvokeExpr) fieldArg).getMethodName())) {
                checkInnerDocMethod((SQLMethodInvokeExpr) fieldArg);
            }
            if (NESTED_DOC_METHOD.equalsIgnoreCase(((SQLMethodInvokeExpr) fieldArg).getMethodName())) {
                checkNestedDocMethod((SQLMethodInvokeExpr) fieldArg);
            }
        } else if (!(fieldArg instanceof SQLPropertyExpr) && !(fieldArg instanceof SQLIdentifierExpr)) {
            throw new ElasticSql2DslException("[syntax error] The first arg of nvl method should be field param name");
        }

        if (!(valueArg instanceof SQLCharExpr) && !(valueArg instanceof SQLIntegerExpr) && !(valueArg instanceof SQLNumberExpr)) {
            throw new ElasticSql2DslException("[syntax error] The second arg of nvl method should be number or string");
        }

        if (nvlInvokeExpr.getParameters().size() == 3) {
            SQLExpr sortModArg = nvlInvokeExpr.getParameters().get(2);
            if (!(sortModArg instanceof SQLCharExpr)) {
                throw new ElasticSql2DslException("[syntax error] The third arg of nvl method should be string");
            }
            String sortModeText = ((SQLCharExpr) sortModArg).getText();
            if (!QueryOrderConditionParser.SortOption.AVG.mode().equalsIgnoreCase(sortModeText) && !QueryOrderConditionParser.SortOption.MIN.mode().equalsIgnoreCase(sortModeText)
                    && !QueryOrderConditionParser.SortOption.MAX.mode().equalsIgnoreCase(sortModeText) && !QueryOrderConditionParser.SortOption.SUM.mode().equalsIgnoreCase(sortModeText)) {
                throw new ElasticSql2DslException("[syntax error] The third arg of nvl method should be one of the string[min,max,avg,sum]");
            }
        }
    }
}
