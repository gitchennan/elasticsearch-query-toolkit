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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.util.List;
import java.util.Map;

public class TermsAtomQueryParser extends AbstractAtomMethodQueryParser {

    private static List<String> TERMS_QUERY_METHOD = ImmutableList.of("terms", "terms_query", "termsQuery");

    public TermsAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public static Boolean isTermsQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(TERMS_QUERY_METHOD, methodQueryExpr.getMethodName());
    }

    @Override
    protected void checkQueryMethod(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        if (Boolean.FALSE == isTermsQuery(methodQueryExpr)) {
            throw new ElasticSql2DslException(String.format("[syntax error] Expected terms query method name is [terms],but get [%s]", methodQueryExpr.getMethodName()));
        }

        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount <= 1) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: term", paramCount));
        }

        for (int idx = 1; idx < paramCount - 1; idx++) {
            SQLExpr textExpr = methodQueryExpr.getParameters().get(idx);

            String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();
            if (StringUtils.isEmpty(text)) {
                throw new ElasticSql2DslException("[syntax error] Terms text can not be blank!");
            }
        }

        SQLExpr lastParam = methodQueryExpr.getParameters().get(paramCount - 1);
        String lastParamText = ElasticSqlArgTransferHelper.transferSqlArg(lastParam, sqlArgs, false).toString();

        if (Boolean.FALSE == isExtraParamsString(lastParamText) && StringUtils.isEmpty(lastParamText)) {
            throw new ElasticSql2DslException("[syntax error] Terms text can not be blank!");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected AtomQuery parseMethodQueryExpr(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        SQLExpr queryField = methodQueryExpr.getParameters().get(0);
        int paramCount = methodQueryExpr.getParameters().size();

        List<String> termTextList = Lists.newArrayList();
        for (int idx = 1; idx < paramCount - 1; idx++) {
            SQLExpr textExpr = methodQueryExpr.getParameters().get(idx);
            String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();
            termTextList.add(text);
        }


        SQLExpr lastParam = methodQueryExpr.getParameters().get(paramCount - 1);
        String lastParamText = ElasticSqlArgTransferHelper.transferSqlArg(lastParam, sqlArgs, false).toString();

        Map<String, String> extraParamMap = null;
        if (isExtraParamsString(lastParamText)) {
            String extraParam = ElasticSqlArgTransferHelper.transferSqlArg(lastParam, sqlArgs, false).toString();

            extraParamMap = buildExtraMethodQueryParamsMap(extraParam);
        }
        else {
            termTextList.add(lastParamText);
        }


        return parseCondition(queryField, new Object[]{termTextList, extraParamMap}, queryAs, new IConditionMethodQueryBuilder() {
            @Override
            public QueryBuilder buildQuery(String queryFieldName, Object[] parameters) {
                TermsQueryBuilder termsQuery = QueryBuilders.termsQuery(queryFieldName, (List<String>) parameters[0]);

                if (parameters.length == 2 && parameters[1] != null) {
                    Map<String, String> tExtraParamMap = (Map<String, String>) parameters[1];
                    setExtraMatchQueryParam(termsQuery, tExtraParamMap);
                }

                return termsQuery;
            }
        });
    }

    private void setExtraMatchQueryParam(TermsQueryBuilder termsQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            termsQuery.boost(Float.valueOf(val));
        }
        if (extraParamMap.containsKey("minimum_should_match")) {
            String val = extraParamMap.get("minimum_should_match");
            termsQuery.minimumShouldMatch(val);
        }
        if (extraParamMap.containsKey("disable_coord")) {
            String val = extraParamMap.get("disable_coord");
            termsQuery.disableCoord(Boolean.parseBoolean(val));
        }


    }
}
