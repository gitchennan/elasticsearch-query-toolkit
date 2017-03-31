package org.elasticsearch.dsl.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.helper.ElasticSqlMethodInvokeHelper;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import org.elasticsearch.dsl.parser.query.method.MethodInvocation;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.util.List;
import java.util.Map;

public class TermsAtomQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static List<String> TERMS_QUERY_METHOD = ImmutableList.of("terms", "terms_query", "termsQuery");

    public TermsAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(TERMS_QUERY_METHOD, invocation.getMatchQueryExpr().getMethodName());
    }

    @Override
    protected String getExtraParamString(MethodInvocation invocation) {
        int paramCount = invocation.getMatchQueryExpr().getParameters().size();
        SQLExpr lastParam = invocation.getMatchQueryExpr().getParameters().get(paramCount - 1);

        String extraParamString = ElasticSqlArgTransferHelper.transferSqlArg(lastParam, invocation.getSqlArgs(), false).toString();

        if (isExtraParamsString(extraParamString)) {
            return extraParamString;
        }
        return StringUtils.EMPTY;
    }

    @Override
    protected void checkQueryMethod(MethodInvocation invocation) {
        if (Boolean.FALSE == isMatchMethodInvocation(invocation)) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] Expected terms query method name is [terms],but get [%s]",
                            invocation.getMatchQueryExpr().getMethodName()));
        }

        SQLMethodInvokeExpr methodQueryExpr = invocation.getMatchQueryExpr();

        int paramCount = invocation.getMatchQueryExpr().getParameters().size();
        if (paramCount <= 1) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: term", paramCount));
        }

        for (int idx = 1; idx < paramCount - 1; idx++) {
            SQLExpr textExpr = methodQueryExpr.getParameters().get(idx);

            String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, invocation.getSqlArgs(), false).toString();
            if (StringUtils.isEmpty(text)) {
                throw new ElasticSql2DslException("[syntax error] Terms text can not be blank!");
            }
        }
    }

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {

        SQLMethodInvokeExpr methodQueryExpr = invocation.getMatchQueryExpr();
        Object[] sqlArgs = invocation.getSqlArgs();

        int paramCount = methodQueryExpr.getParameters().size();

        List<String> termTextList = Lists.newArrayList();
        for (int idx = 1; idx < paramCount - 1; idx++) {
            SQLExpr textExpr = methodQueryExpr.getParameters().get(idx);
            String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();
            termTextList.add(text);
        }


        SQLExpr lastParam = methodQueryExpr.getParameters().get(paramCount - 1);
        String lastParamText = ElasticSqlArgTransferHelper.transferSqlArg(lastParam, sqlArgs, false).toString();

        if (Boolean.FALSE == isExtraParamsString(lastParamText)) {
            termTextList.add(lastParamText);
        }

        TermsQueryBuilder termsQuery = QueryBuilders.termsQuery(fieldName, termTextList);
        setExtraMatchQueryParam(termsQuery, extraParams);
        return termsQuery;
    }

    @Override
    protected SQLExpr getFieldExpr(MethodInvocation invocation) {
        return invocation.getMatchQueryExpr().getParameters().get(0);
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
