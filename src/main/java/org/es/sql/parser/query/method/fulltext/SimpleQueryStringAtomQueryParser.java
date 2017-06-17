package org.es.sql.parser.query.method.fulltext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.SimpleQueryStringFlag;
import org.es.sql.bean.AtomQuery;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.parser.query.method.MethodInvocation;
import org.es.sql.parser.query.method.ParameterizedMethodQueryParser;

import java.util.List;
import java.util.Map;

public class SimpleQueryStringAtomQueryParser extends ParameterizedMethodQueryParser {

    private static List<String> SIMPLE_QUERY_STRING_METHOD = ImmutableList.of("simpleQueryString", "simple_query_string");

    @Override
    public List<String> defineMethodNames() {
        return SIMPLE_QUERY_STRING_METHOD;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        return isExtraParamsString(invocation.getLastParameterAsString())
                ? invocation.getLastParameterAsString() : StringUtils.EMPTY;
    }

    @Override
    protected AtomQuery parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        String text = invocation.getParameterAsString(0);
        SimpleQueryStringBuilder simpleQueryString = QueryBuilders.simpleQueryStringQuery(text);

        String queryFields = null;
        if (invocation.getParameterCount() == 3) {
            queryFields = invocation.getParameterAsString(1);

            if (StringUtils.isNotBlank(queryFields)) {
                String[] tArr = queryFields.split(COLON);
                if ("fields".equalsIgnoreCase(tArr[0])) {
                    for (String fieldItem : tArr[1].split(COMMA)) {
                        simpleQueryString.field(fieldItem);
                    }
                }
            }
        }

        if (MapUtils.isNotEmpty(extraParamMap)) {
            setExtraMatchQueryParam(simpleQueryString, extraParamMap);
        }

        return new AtomQuery(simpleQueryString);
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        int paramCount = invocation.getParameterCount();
        if (paramCount != 1 && paramCount != 2 && paramCount != 3) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String text = invocation.getParameterAsString(0);

        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Search text can not be blank!");
        }

        if (paramCount == 3) {
            String strFields = invocation.getParameterAsString(1);

            if (StringUtils.isEmpty(text)) {
                throw new ElasticSql2DslException("[syntax error] Search fields can not be empty!");
            }
            String[] tArr = strFields.split(COLON);

            if (tArr.length != 2) {
                throw new ElasticSql2DslException("[syntax error] queryString method args error");
            }

            if (Boolean.FALSE == "fields".equalsIgnoreCase(tArr[0])) {
                throw new ElasticSql2DslException("[syntax error] Search fields name should one of [fields]");
            }
        }
    }

    private void setExtraMatchQueryParam(SimpleQueryStringBuilder simpleStringQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }

        if (extraParamMap.containsKey("minimum_should_match")) {
            String val = extraParamMap.get("minimum_should_match");
            simpleStringQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey("analyzer")) {
            String val = extraParamMap.get("analyzer");
            simpleStringQuery.analyzer(val);
        }

//        if (extraParamMap.containsKey("lowercase_expanded_terms")) {
//            String val = extraParamMap.get("lowercase_expanded_terms");
//            simpleStringQuery.lowercaseExpandedTerms(Boolean.parseBoolean(val));
//        }

        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            simpleStringQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey("analyze_wildcard")) {
            String val = extraParamMap.get("analyze_wildcard");
            simpleStringQuery.analyzeWildcard(Boolean.parseBoolean(val));
        }

//        if (extraParamMap.containsKey("locale")) {
//            String val = extraParamMap.get("locale");
//            simpleStringQuery.locale(Locale.forLanguageTag(val));
//        }

        if (extraParamMap.containsKey("flags")) {
            String[] flags = extraParamMap.get("flags").split("\\|");
            List<SimpleQueryStringFlag> flagList = Lists.newLinkedList();
            for (String flag : flags) {
                flagList.add(SimpleQueryStringFlag.valueOf(flag.toUpperCase()));
            }
            simpleStringQuery.flags(flagList.toArray(new SimpleQueryStringFlag[flagList.size()]));
        }


        if (extraParamMap.containsKey("default_operator")) {
            String val = extraParamMap.get("default_operator");

            if ("AND".equalsIgnoreCase(val)) {
                simpleStringQuery.defaultOperator(Operator.AND);
            }
            if ("OR".equalsIgnoreCase(val)) {
                simpleStringQuery.defaultOperator(Operator.OR);
            }
        }
    }
}
