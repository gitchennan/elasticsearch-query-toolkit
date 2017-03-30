package org.elasticsearch.dsl.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.helper.ElasticSqlMethodInvokeHelper;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractAtomMethodQueryParser;
import org.elasticsearch.dsl.parser.query.method.IConditionMethodQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.index.query.RegexpQueryBuilder;

import java.util.List;
import java.util.Map;

public class RegexpAtomQueryParser extends AbstractAtomMethodQueryParser {

    private static List<String> REGEXP_QUERY_METHOD = ImmutableList.of("regexp", "regexp_query", "regexpQuery");

    public RegexpAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public static Boolean isRegexpQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(REGEXP_QUERY_METHOD, methodQueryExpr.getMethodName());
    }

    @Override
    protected void checkQueryMethod(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        if (Boolean.FALSE == isRegexpQuery(methodQueryExpr)) {
            throw new ElasticSql2DslException(String.format("[syntax error] Expected regexp query method name is [regexp],but get [%s]", methodQueryExpr.getMethodName()));
        }

        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount != 2 && paramCount != 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: regexp", paramCount));
        }

        SQLExpr textExpr = methodQueryExpr.getParameters().get(1);

        String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Regexp text can not be blank!");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected AtomQuery parseMethodQueryExpr(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        SQLExpr queryField = methodQueryExpr.getParameters().get(0);
        SQLExpr textExpr = methodQueryExpr.getParameters().get(1);

        Map<String, String> extraParamMap = null;
        if (methodQueryExpr.getParameters().size() == 3) {
            SQLExpr ExtraParamExpr = methodQueryExpr.getParameters().get(2);
            String extraParam = ElasticSqlArgTransferHelper.transferSqlArg(ExtraParamExpr, sqlArgs, false).toString();

            extraParamMap = buildExtraMethodQueryParamsMap(extraParam);
        }

        Object text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false);

        return parseCondition(queryField, new Object[]{text, extraParamMap}, queryAs, new IConditionMethodQueryBuilder() {
            @Override
            public QueryBuilder buildQuery(String queryFieldName, Object[] parameters) {
                RegexpQueryBuilder regexpQuery = QueryBuilders.regexpQuery(queryFieldName, parameters[0].toString());

                if (parameters.length == 2 && parameters[1] != null) {
                    Map<String, String> tExtraParamMap = (Map<String, String>) parameters[1];
                    setExtraMatchQueryParam(regexpQuery, tExtraParamMap);
                }

                return regexpQuery;
            }
        });
    }

    private void setExtraMatchQueryParam(RegexpQueryBuilder regexpQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            regexpQuery.boost(Float.valueOf(val));
        }
        if (extraParamMap.containsKey("rewrite")) {
            String val = extraParamMap.get("rewrite");
            regexpQuery.rewrite(val);
        }
        if (extraParamMap.containsKey("max_determinized_states")) {
            String val = extraParamMap.get("max_determinized_states");
            regexpQuery.maxDeterminizedStates(Integer.valueOf(val));
        }
        if (extraParamMap.containsKey("flags")) {
            String[] flags = extraParamMap.get("flags").split("\\|");
            List<RegexpFlag> flagList = Lists.newLinkedList();
            for (String flag : flags) {
                flagList.add(RegexpFlag.valueOf(flag.toUpperCase()));
            }
            regexpQuery.flags(flagList.toArray(new RegexpFlag[flagList.size()]));
        }
        if (extraParamMap.containsKey("flags_value")) {
            String[] flags = extraParamMap.get("flags_value").split("\\|");
            List<RegexpFlag> flagList = Lists.newLinkedList();
            for (String flag : flags) {
                flagList.add(RegexpFlag.valueOf(flag.toUpperCase()));
            }
            regexpQuery.flags(flagList.toArray(new RegexpFlag[flagList.size()]));
        }
    }
}
