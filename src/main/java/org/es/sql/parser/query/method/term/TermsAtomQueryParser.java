package org.es.sql.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.listener.ParseActionListener;
import org.es.sql.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import org.es.sql.parser.query.method.MethodInvocation;


import java.util.List;
import java.util.Map;

public class TermsAtomQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static List<String> TERMS_QUERY_METHOD = ImmutableList.of("terms", "terms_query", "termsQuery");

    public TermsAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public List<String> defineMethodNames() {
        return TERMS_QUERY_METHOD;
    }

    @Override
    public SQLExpr defineFieldExpr(MethodInvocation invocation) {
        return invocation.getParameter(0);
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        String extraParamString = invocation.getLastParameterAsString();
        if (isExtraParamsString(extraParamString)) {
            return extraParamString;
        }
        return StringUtils.EMPTY;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) {
        if (invocation.getParameterCount() <= 1) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        int paramCount = invocation.getParameterCount();

        for (int idx = 1; idx < paramCount - 1; idx++) {
            String text = invocation.getParameterAsString(idx);
            if (StringUtils.isEmpty(text)) {
                throw new ElasticSql2DslException("[syntax error] Terms text can not be blank!");
            }
        }
    }

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        int paramCount = invocation.getParameterCount();

        List<String> termTextList = Lists.newArrayList();
        for (int idx = 1; idx < paramCount - 1; idx++) {
            String text = invocation.getParameterAsString(idx);
            termTextList.add(text);
        }

        String lastParamText = invocation.getLastParameterAsString();
        if (!isExtraParamsString(lastParamText)) {
            termTextList.add(lastParamText);
        }

        TermsQueryBuilder termsQuery = QueryBuilders.termsQuery(fieldName, termTextList);
        setExtraMatchQueryParam(termsQuery, extraParams);
        return termsQuery;
    }

    private void setExtraMatchQueryParam(TermsQueryBuilder termsQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            termsQuery.boost(Float.valueOf(val));
        }
//        if (extraParamMap.containsKey("minimum_should_match")) {
//            String val = extraParamMap.get("minimum_should_match");
//            termsQuery.minimumShouldMatch(val);
//        }
//        if (extraParamMap.containsKey("disable_coord")) {
//            String val = extraParamMap.get("disable_coord");
//            termsQuery.disableCoord(Boolean.parseBoolean(val));
//        }


    }
}
