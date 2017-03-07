package org.elasticsearch.dsl.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractAtomMethodQueryParser;
import org.elasticsearch.dsl.parser.query.method.IConditionMethodQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Map;

public class MatchAtomQueryParser extends AbstractAtomMethodQueryParser {

    public MatchAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    protected void checkQueryMethod(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        if (Boolean.FALSE == "match".equalsIgnoreCase(methodQueryExpr.getMethodName())) {
            throw new ElasticSql2DslException(String.format("[syntax error] Expected match query method name is [match],but get [%s]", methodQueryExpr.getMethodName()));
        }

        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount != 2 && paramCount != 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: match", paramCount));
        }

        SQLExpr textExpr = methodQueryExpr.getParameters().get(1);

        String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Search text can not be blank!");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected AtomQuery parseMethodQueryExpr(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        SQLExpr queryField = methodQueryExpr.getParameters().get(0);
        SQLExpr textExpr = methodQueryExpr.getParameters().get(1);

        Map<String, String> extraParamMap = null;
        if (methodQueryExpr.getParameters().size() == 3) {
            SQLExpr extraParamExpr = methodQueryExpr.getParameters().get(2);
            String extraParam = ElasticSqlArgTransferHelper.transferSqlArg(extraParamExpr, sqlArgs, false).toString();

            extraParamMap = buildExtraMethodQueryParamsMap(extraParam);
        }

        Object text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false);

        return parseCondition(queryField, new Object[]{text, extraParamMap}, queryAs, new IConditionMethodQueryBuilder() {
            @Override
            public QueryBuilder buildQuery(String queryFieldName, Object[] parameters) {
                MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(queryFieldName, parameters[0]);

                if (parameters.length == 2 && parameters[1] != null) {
                    Map<String, String> tExtraParamMap = (Map<String, String>) parameters[1];
                    setExtraMatchQueryParam(matchQuery, tExtraParamMap);
                }

                return matchQuery;
            }
        });
    }

    private void setExtraMatchQueryParam(MatchQueryBuilder matchQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("type")) {
            String val = extraParamMap.get("type");
            if ("BOOLEAN".equalsIgnoreCase(val)) {
                matchQuery.type(MatchQueryBuilder.Type.BOOLEAN);
            }
            if ("PHRASE".equalsIgnoreCase(val)) {
                matchQuery.type(MatchQueryBuilder.Type.PHRASE);
            }
            if ("PHRASE_PREFIX".equalsIgnoreCase(val)) {
                matchQuery.type(MatchQueryBuilder.Type.PHRASE_PREFIX);
            }
        }

        if (extraParamMap.containsKey("operator")) {
            String val = extraParamMap.get("operator");
            if ("AND".equalsIgnoreCase(val)) {
                matchQuery.operator(MatchQueryBuilder.Operator.AND);
            }
            if ("OR".equalsIgnoreCase(val)) {
                matchQuery.operator(MatchQueryBuilder.Operator.OR);
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
                matchQuery.zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE);
            }
            if ("ALL".equalsIgnoreCase(val)) {
                matchQuery.zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL);
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
