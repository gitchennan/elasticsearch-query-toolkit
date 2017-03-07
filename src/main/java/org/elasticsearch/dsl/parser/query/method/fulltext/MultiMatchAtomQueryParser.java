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
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Map;

public class MultiMatchAtomQueryParser extends AbstractAtomMethodQueryParser {

    public MultiMatchAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    protected void checkQueryMethod(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        if (Boolean.FALSE == "multiMatch".equalsIgnoreCase(methodQueryExpr.getMethodName())) {
            throw new ElasticSql2DslException(String.format("[syntax error] Expected multiMatch query method name is [multiMatch],but get [%s]", methodQueryExpr.getMethodName()));
        }

        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount != 2 && paramCount != 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: multiMatch", paramCount));
        }

        SQLExpr fieldsExpr = methodQueryExpr.getParameters().get(0);
        SQLExpr textExpr = methodQueryExpr.getParameters().get(1);

        String strFields = ElasticSqlArgTransferHelper.transferSqlArg(fieldsExpr, sqlArgs, false).toString();
        String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();

        if (StringUtils.isEmpty(strFields)) {
            throw new ElasticSql2DslException("[syntax error] Search fields can not be empty!");
        }
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Search text can not be blank!");
        }
    }

    @Override
    protected AtomQuery parseMethodQueryExpr(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        SQLExpr queryFields = methodQueryExpr.getParameters().get(0);
        SQLExpr textExpr = methodQueryExpr.getParameters().get(1);

        Map<String, String> extraParamMap = null;
        if (methodQueryExpr.getParameters().size() == 3) {
            SQLExpr extraParamExpr = methodQueryExpr.getParameters().get(2);
            String extraParam = ElasticSqlArgTransferHelper.transferSqlArg(extraParamExpr, sqlArgs, false).toString();

            extraParamMap = buildExtraMethodQueryParamsMap(extraParam);
        }

        String[] fields = ElasticSqlArgTransferHelper.transferSqlArg(queryFields, sqlArgs, false).toString().split(COMMA);
        Object text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false);

        MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(text, fields);
        setExtraMatchQueryParam(multiMatchQuery, extraParamMap);

        return new AtomQuery(multiMatchQuery);
    }

    private void setExtraMatchQueryParam(MultiMatchQueryBuilder multiMatchQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("type")) {
            String val = extraParamMap.get("type");
            if ("BOOLEAN".equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQueryBuilder.Type.BOOLEAN);
            }
            if ("PHRASE".equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQueryBuilder.Type.PHRASE);
            }
            if ("PHRASE_PREFIX".equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQueryBuilder.Type.PHRASE_PREFIX);
            }
        }

        if (extraParamMap.containsKey("operator")) {
            String val = extraParamMap.get("operator");
            if ("AND".equalsIgnoreCase(val)) {
                multiMatchQuery.operator(MatchQueryBuilder.Operator.AND);
            }
            if ("OR".equalsIgnoreCase(val)) {
                multiMatchQuery.operator(MatchQueryBuilder.Operator.OR);
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
                multiMatchQuery.zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.NONE);
            }
            if ("ALL".equalsIgnoreCase(val)) {
                multiMatchQuery.zeroTermsQuery(MatchQueryBuilder.ZeroTermsQuery.ALL);
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
