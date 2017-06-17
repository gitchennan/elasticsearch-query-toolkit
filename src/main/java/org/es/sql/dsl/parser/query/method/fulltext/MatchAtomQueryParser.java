package org.es.sql.dsl.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.search.MatchQuery;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.listener.ParseActionListener;
import org.es.sql.dsl.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import org.es.sql.dsl.parser.query.method.MethodInvocation;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;
import java.util.Map;

public class MatchAtomQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static final List<String> MATCH_METHOD = ImmutableList.of("match", "match_query", "matchQuery");

    public MatchAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public List<String> defineMethodNames() {
        return MATCH_METHOD;
    }

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        String text = invocation.getParameterAsString(1);
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(fieldName, text);

        setExtraMatchQueryParam(matchQuery, extraParams);
        return matchQuery;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        int extraParamIdx = 2;

        return (invocation.getParameterCount() == extraParamIdx + 1)
                ? invocation.getParameterAsString(extraParamIdx) : StringUtils.EMPTY;
    }

    @Override
    public SQLExpr defineFieldExpr(MethodInvocation invocation) {
        return invocation.getParameter(0);
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 2 && invocation.getParameterCount() != 3) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String text = invocation.getParameterAsString(1);
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Match search text can not be blank!");
        }

        if (invocation.getParameterCount() == 3) {
            String extraParamString = defineExtraParamString(invocation);
            if (StringUtils.isEmpty(extraParamString)) {
                throw new ElasticSql2DslException("[syntax error] The extra param of match method can not be blank");
            }
        }
    }

    private void setExtraMatchQueryParam(MatchQueryBuilder matchQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("type")) {
            String val = extraParamMap.get("type");
            if ("BOOLEAN".equalsIgnoreCase(val)) {
                matchQuery.type(MatchQuery.Type.BOOLEAN);
            }
            if ("PHRASE".equalsIgnoreCase(val)) {
                matchQuery.type(MatchQuery.Type.PHRASE);
            }
            if ("PHRASE_PREFIX".equalsIgnoreCase(val)) {
                matchQuery.type(MatchQuery.Type.PHRASE_PREFIX);
            }
        }

        if (extraParamMap.containsKey("operator")) {
            String val = extraParamMap.get("operator");
            if ("AND".equalsIgnoreCase(val)) {
                matchQuery.operator(Operator.AND);
            }
            if ("OR".equalsIgnoreCase(val)) {
                matchQuery.operator(Operator.OR);
            }
        }

        if (extraParamMap.containsKey("minimum_should_match")) {
            String val = extraParamMap.get("minimum_should_match");
            matchQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey("analyzer")) {
            String val = extraParamMap.get("analyzer");
            matchQuery.analyzer(val);
        }

        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            matchQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey("slop")) {
            String val = extraParamMap.get("slop");
            matchQuery.slop(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey("prefix_length")) {
            String val = extraParamMap.get("prefix_length");
            matchQuery.prefixLength(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey("max_expansions")) {
            String val = extraParamMap.get("max_expansions");

            matchQuery.maxExpansions(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey("fuzzy_rewrite")) {
            String val = extraParamMap.get("fuzzy_rewrite");
            matchQuery.fuzzyRewrite(val);
        }

        if (extraParamMap.containsKey("fuzzy_transpositions")) {
            String val = extraParamMap.get("fuzzy_transpositions");
            matchQuery.fuzzyTranspositions(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("lenient")) {
            String val = extraParamMap.get("lenient");
            matchQuery.setLenient(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("zero_terms_query")) {
            String val = extraParamMap.get("zero_terms_query");
            if ("NONE".equalsIgnoreCase(val)) {
                matchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE);
            }
            if ("ALL".equalsIgnoreCase(val)) {
                matchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);
            }
        }

        if (extraParamMap.containsKey("cutoff_Frequency")) {
            String val = extraParamMap.get("cutoff_Frequency");
            matchQuery.cutoffFrequency(Float.valueOf(val));
        }

        if (extraParamMap.containsKey("fuzziness")) {
            String val = extraParamMap.get("fuzziness");

            if ("ZERO".equalsIgnoreCase(val)) {
                matchQuery.fuzziness(Fuzziness.ZERO);
            }
            if ("ONE".equalsIgnoreCase(val)) {
                matchQuery.fuzziness(Fuzziness.ONE);
            }

            if ("TWO".equalsIgnoreCase(val)) {
                matchQuery.fuzziness(Fuzziness.TWO);
            }

            if ("AUTO".equalsIgnoreCase(val)) {
                matchQuery.fuzziness(Fuzziness.AUTO);
            }
        }
    }
}
