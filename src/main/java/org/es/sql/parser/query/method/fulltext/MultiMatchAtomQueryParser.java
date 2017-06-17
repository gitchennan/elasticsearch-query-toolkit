package org.es.sql.parser.query.method.fulltext;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;
import org.es.sql.bean.AtomQuery;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.parser.query.method.MethodInvocation;
import org.es.sql.parser.query.method.ParameterizedMethodQueryParser;

import java.util.List;
import java.util.Map;

public class MultiMatchAtomQueryParser extends ParameterizedMethodQueryParser {

    private static final List<String> MULTI_MATCH_METHOD = ImmutableList.of("multiMatch", "multi_match", "multi_match_query", "multiMatchQuery");

    @Override
    public List<String> defineMethodNames() {
        return MULTI_MATCH_METHOD;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        int extraParamIdx = 2;

        return (invocation.getParameterCount() == extraParamIdx + 1)
                ? invocation.getParameterAsString(extraParamIdx) : StringUtils.EMPTY;
    }

    @Override
    protected AtomQuery parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        String[] fields = invocation.getParameterAsString(0).split(COMMA);
        String text = invocation.getParameterAsString(1);

        MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(text, fields);
        setExtraMatchQueryParam(multiMatchQuery, extraParamMap);

        return new AtomQuery(multiMatchQuery);
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 2 && invocation.getParameterCount() != 3) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String strFields = invocation.getParameterAsString(0);
        String text = invocation.getParameterAsString(1);

        if (StringUtils.isEmpty(strFields)) {
            throw new ElasticSql2DslException("[syntax error] Search fields can not be empty!");
        }
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Search text can not be blank!");
        }
    }

    private void setExtraMatchQueryParam(MultiMatchQueryBuilder multiMatchQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("type")) {
            String val = extraParamMap.get("type");
            if ("BOOLEAN".equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQuery.Type.BOOLEAN);
            }
            if ("PHRASE".equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQuery.Type.PHRASE);
            }
            if ("PHRASE_PREFIX".equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQuery.Type.PHRASE_PREFIX);
            }
        }

        if (extraParamMap.containsKey("operator")) {
            String val = extraParamMap.get("operator");
            if ("AND".equalsIgnoreCase(val)) {
                multiMatchQuery.operator(Operator.AND);
            }
            if ("OR".equalsIgnoreCase(val)) {
                multiMatchQuery.operator(Operator.OR);
            }
        }

        if (extraParamMap.containsKey("minimum_should_match")) {
            String val = extraParamMap.get("minimum_should_match");
            multiMatchQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey("analyzer")) {
            String val = extraParamMap.get("analyzer");
            multiMatchQuery.analyzer(val);
        }

        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            multiMatchQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey("slop")) {
            String val = extraParamMap.get("slop");
            multiMatchQuery.slop(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey("prefix_length")) {
            String val = extraParamMap.get("prefix_length");
            multiMatchQuery.prefixLength(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey("max_expansions")) {
            String val = extraParamMap.get("max_expansions");

            multiMatchQuery.maxExpansions(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey("fuzzy_rewrite")) {
            String val = extraParamMap.get("fuzzy_rewrite");
            multiMatchQuery.fuzzyRewrite(val);
        }

        if (extraParamMap.containsKey("use_dis_max")) {
            String val = extraParamMap.get("use_dis_max");
            multiMatchQuery.useDisMax(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("tie_breaker")) {
            String val = extraParamMap.get("tie_breaker");
            multiMatchQuery.tieBreaker(Float.valueOf(val));
        }

        if (extraParamMap.containsKey("zero_terms_query")) {
            String val = extraParamMap.get("zero_terms_query");
            if ("NONE".equalsIgnoreCase(val)) {
                multiMatchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE);
            }
            if ("ALL".equalsIgnoreCase(val)) {
                multiMatchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);
            }
        }

        if (extraParamMap.containsKey("cutoff_Frequency")) {
            String val = extraParamMap.get("cutoff_Frequency");
            multiMatchQuery.cutoffFrequency(Float.valueOf(val));
        }

        if (extraParamMap.containsKey("fuzziness")) {
            String val = extraParamMap.get("fuzziness");

            if ("ZERO".equalsIgnoreCase(val) || "0".equals(val)) {
                multiMatchQuery.fuzziness(Fuzziness.ZERO);
            }
            if ("ONE".equalsIgnoreCase(val) || "1".equals(val)) {
                multiMatchQuery.fuzziness(Fuzziness.ONE);
            }

            if ("TWO".equalsIgnoreCase(val) || "2".equals(val)) {
                multiMatchQuery.fuzziness(Fuzziness.TWO);
            }

            if ("AUTO".equalsIgnoreCase(val)) {
                multiMatchQuery.fuzziness(Fuzziness.AUTO);
            }
        }
    }
}
