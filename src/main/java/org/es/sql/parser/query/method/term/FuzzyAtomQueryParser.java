package org.es.sql.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.listener.ParseActionListener;
import org.es.sql.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import org.es.sql.parser.query.method.MethodInvocation;


import java.util.List;
import java.util.Map;

public class FuzzyAtomQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static final List<String> FUZZY_QUERY_METHOD = ImmutableList.of("fuzzy", "fuzzy_query", "fuzzyQuery");

    public FuzzyAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public List<String> defineMethodNames() {
        return FUZZY_QUERY_METHOD;
    }

    @Override
    public SQLExpr defineFieldExpr(MethodInvocation invocation) {
        return invocation.getParameter(0);
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        int extraParamIdx = 2;

        return (invocation.getParameterCount() == extraParamIdx + 1)
                ? invocation.getParameterAsString(extraParamIdx) : StringUtils.EMPTY;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) {
        if (invocation.getParameterCount() != 2 && invocation.getParameterCount() != 3) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String text = invocation.getParameterAsString(1);
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Fuzzy search text can not be blank!");
        }

        if (invocation.getParameterCount() == 3) {
            String extraParamString = defineExtraParamString(invocation);
            if (StringUtils.isEmpty(extraParamString)) {
                throw new ElasticSql2DslException("[syntax error] The extra param of fuzzy method can not be blank");
            }
        }
    }

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        String text = invocation.getParameterAsString(1);
        FuzzyQueryBuilder fuzzyQuery = QueryBuilders.fuzzyQuery(fieldName, text);

        setExtraMatchQueryParam(fuzzyQuery, extraParams);
        return fuzzyQuery;
    }

    private void setExtraMatchQueryParam(FuzzyQueryBuilder fuzzyQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            fuzzyQuery.boost(Float.valueOf(val));
        }
        if (extraParamMap.containsKey("transpositions")) {
            String val = extraParamMap.get("transpositions");
            fuzzyQuery.transpositions(Boolean.parseBoolean(val));
        }
        if (extraParamMap.containsKey("prefix_length")) {
            String val = extraParamMap.get("prefix_length");
            fuzzyQuery.prefixLength(Integer.valueOf(val));
        }
        if (extraParamMap.containsKey("max_expansions")) {
            String val = extraParamMap.get("max_expansions");
            fuzzyQuery.maxExpansions(Integer.valueOf(val));
        }
        if (extraParamMap.containsKey("rewrite")) {
            String val = extraParamMap.get("rewrite");
            fuzzyQuery.rewrite(val);
        }

        if (extraParamMap.containsKey("fuzziness")) {
            String val = extraParamMap.get("fuzziness");

            if ("ZERO".equalsIgnoreCase(val) || "0".equals(val)) {
                fuzzyQuery.fuzziness(Fuzziness.ZERO);
            }
            if ("ONE".equalsIgnoreCase(val) || "1".equals(val)) {
                fuzzyQuery.fuzziness(Fuzziness.ONE);
            }

            if ("TWO".equalsIgnoreCase(val) || "2".equals(val)) {
                fuzzyQuery.fuzziness(Fuzziness.TWO);
            }

            if ("AUTO".equalsIgnoreCase(val)) {
                fuzzyQuery.fuzziness(Fuzziness.AUTO);
            }
        }
    }
}
