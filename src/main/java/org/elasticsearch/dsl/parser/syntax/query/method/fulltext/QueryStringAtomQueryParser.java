package org.elasticsearch.dsl.parser.syntax.query.method.fulltext;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.syntax.query.method.AbstractAtomMethodQueryParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import java.util.Locale;
import java.util.Map;

public class QueryStringAtomQueryParser extends AbstractAtomMethodQueryParser {

    public QueryStringAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    protected void checkQueryMethod(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs) {
        if (Boolean.FALSE == "queryString".equalsIgnoreCase(matchQueryExpr.getMethodName())) {
            throw new ElasticSql2DslException(String.format("[syntax error] Expected queryString query method name is [queryString],but get [%s]", matchQueryExpr.getMethodName()));
        }

        int paramCount = matchQueryExpr.getParameters().size();
        if (paramCount != 1 && paramCount != 2 && paramCount != 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: queryString", paramCount));
        }

        SQLExpr textExpr = matchQueryExpr.getParameters().get(0);
        String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();

        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Search text can not be blank!");
        }

        if (paramCount == 3) {
            SQLExpr fieldsExpr = matchQueryExpr.getParameters().get(1);
            String strFields = ElasticSqlArgTransferHelper.transferSqlArg(fieldsExpr, sqlArgs, false).toString();

            if (StringUtils.isEmpty(text)) {
                throw new ElasticSql2DslException("[syntax error] Search fields can not be empty!");
            }
            String[] tArr = strFields.split(COLON);

            if (tArr.length != 2) {
                throw new ElasticSql2DslException("[syntax error] queryString method args error");
            }

            if (Boolean.FALSE == "fields".equalsIgnoreCase(tArr[0]) && Boolean.FALSE == "default_field".equalsIgnoreCase(tArr[0])) {
                throw new ElasticSql2DslException("[syntax error] Search fields name should one of [fields, default_field]");
            }
        }
    }

    @Override
    protected AtomQuery parseMethodQueryExpr(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs) {
        SQLExpr textExpr = matchQueryExpr.getParameters().get(0);

        SQLExpr queryFields = null;
        SQLExpr extraParamExpr = null;

        if (matchQueryExpr.getParameters().size() == 2) {
            extraParamExpr = matchQueryExpr.getParameters().get(1);
        } else if (matchQueryExpr.getParameters().size() == 3) {
            queryFields = matchQueryExpr.getParameters().get(1);
            extraParamExpr = matchQueryExpr.getParameters().get(2);
        }


        String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();
        QueryStringQueryBuilder queryStringQuery = QueryBuilders.queryStringQuery(text);

        if (queryFields != null) {
            String[] tArr = ElasticSqlArgTransferHelper.transferSqlArg(queryFields, sqlArgs, false).toString().split(COLON);
            if ("fields".equalsIgnoreCase(tArr[0])) {
                for (String fieldItem : tArr[1].split(COMMA)) {
                    queryStringQuery.field(fieldItem);
                }
            }

            if ("default_field".equalsIgnoreCase(tArr[0])) {
                queryStringQuery.defaultField(tArr[1]);
            }
        }

        Map<String, String> extraParamMap = null;
        if (extraParamExpr != null) {
            String extraParam = ElasticSqlArgTransferHelper.transferSqlArg(extraParamExpr, sqlArgs, false).toString();
            extraParamMap = buildExtraMatchQueryParamsMap(extraParam);
        }


        setExtraMatchQueryParam(queryStringQuery, extraParamMap);

        return new AtomQuery(queryStringQuery);
    }

    private void setExtraMatchQueryParam(QueryStringQueryBuilder queryStringQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }

        if (extraParamMap.containsKey("minimum_should_match")) {
            String val = extraParamMap.get("minimum_should_match");
            queryStringQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey("analyzer")) {
            String val = extraParamMap.get("analyzer");
            queryStringQuery.analyzer(val);
        }

        if (extraParamMap.containsKey("quote_analyzer")) {
            String val = extraParamMap.get("quote_analyzer");
            queryStringQuery.quoteAnalyzer(val);
        }

        if (extraParamMap.containsKey("auto_generate_phrase_queries")) {
            String val = extraParamMap.get("auto_generate_phrase_queries");
            queryStringQuery.autoGeneratePhraseQueries(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("max_determinized_states")) {
            String val = extraParamMap.get("max_determinized_states");
            queryStringQuery.maxDeterminizedStates(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey("allow_leading_wildcard")) {
            String val = extraParamMap.get("allow_leading_wildcard");
            queryStringQuery.allowLeadingWildcard(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("lowercase_expanded_terms")) {
            String val = extraParamMap.get("lowercase_expanded_terms");
            queryStringQuery.lowercaseExpandedTerms(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("enable_position_increments")) {
            String val = extraParamMap.get("enable_position_increments");
            queryStringQuery.enablePositionIncrements(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("fuzzy_prefix_length")) {
            String val = extraParamMap.get("fuzzy_prefix_length");
            queryStringQuery.fuzzyPrefixLength(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey("fuzzy_max_expansions")) {
            String val = extraParamMap.get("fuzzy_max_expansions");
            queryStringQuery.fuzzyMaxExpansions(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            queryStringQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey("fuzzy_rewrite")) {
            String val = extraParamMap.get("fuzzy_rewrite");
            queryStringQuery.fuzzyRewrite(val);
        }

        if (extraParamMap.containsKey("rewrite")) {
            String val = extraParamMap.get("rewrite");
            queryStringQuery.rewrite(val);
        }

        if (extraParamMap.containsKey("phrase_slop")) {
            String val = extraParamMap.get("phrase_slop");
            queryStringQuery.phraseSlop(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey("analyze_wildcard")) {
            String val = extraParamMap.get("analyze_wildcard");
            queryStringQuery.analyzeWildcard(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("quote_field_suffix")) {
            String val = extraParamMap.get("quote_field_suffix");
            queryStringQuery.quoteFieldSuffix(val);
        }

        if (extraParamMap.containsKey("use_dis_max")) {
            String val = extraParamMap.get("use_dis_max");
            queryStringQuery.useDisMax(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("tie_breaker")) {
            String val = extraParamMap.get("tie_breaker");
            queryStringQuery.tieBreaker(Float.valueOf(val));
        }

        if (extraParamMap.containsKey("time_zone")) {
            String val = extraParamMap.get("time_zone");
            queryStringQuery.timeZone(val);
        }

        if (extraParamMap.containsKey("escape")) {
            String val = extraParamMap.get("escape");
            queryStringQuery.escape(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("locale")) {
            String val = extraParamMap.get("locale");
            queryStringQuery.locale(Locale.forLanguageTag(val));
        }

        if (extraParamMap.containsKey("default_operator")) {
            String val = extraParamMap.get("default_operator");

            if ("AND".equalsIgnoreCase(val)) {
                queryStringQuery.defaultOperator(QueryStringQueryBuilder.Operator.AND);
            }
            if ("OR".equalsIgnoreCase(val)) {
                queryStringQuery.defaultOperator(QueryStringQueryBuilder.Operator.OR);
            }
        }

        if (extraParamMap.containsKey("fuzziness")) {
            String val = extraParamMap.get("fuzziness");

            if ("ZERO".equalsIgnoreCase(val)) {
                queryStringQuery.fuzziness(Fuzziness.ZERO);
            }
            if ("ONE".equalsIgnoreCase(val)) {
                queryStringQuery.fuzziness(Fuzziness.ONE);
            }

            if ("TWO".equalsIgnoreCase(val)) {
                queryStringQuery.fuzziness(Fuzziness.TWO);
            }

            if ("AUTO".equalsIgnoreCase(val)) {
                queryStringQuery.fuzziness(Fuzziness.AUTO);
            }
        }
    }
}
