package org.elasticsearch.dsl.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.Lists;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractAtomMethodQueryParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.SimpleQueryStringFlag;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SimpleQueryStringAtomQueryParser extends AbstractAtomMethodQueryParser {

    public SimpleQueryStringAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    protected void checkQueryMethod(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        if (Boolean.FALSE == "simpleQueryString".equalsIgnoreCase(methodQueryExpr.getMethodName())) {
            throw new ElasticSql2DslException(String.format("[syntax error] Expected simpleQueryString query method name is [simpleQueryString],but get [%s]", methodQueryExpr.getMethodName()));
        }

        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount != 1 && paramCount != 2 && paramCount != 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: queryString", paramCount));
        }

        SQLExpr textExpr = methodQueryExpr.getParameters().get(0);
        String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();

        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Search text can not be blank!");
        }

        if (paramCount == 3) {
            SQLExpr fieldsExpr = methodQueryExpr.getParameters().get(1);
            String strFields = ElasticSqlArgTransferHelper.transferSqlArg(fieldsExpr, sqlArgs, false).toString();

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

    @Override
    protected AtomQuery parseMethodQueryExpr(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        SQLExpr textExpr = methodQueryExpr.getParameters().get(0);

        SQLExpr queryFields = null;
        SQLExpr extraParamExpr = null;

        if (methodQueryExpr.getParameters().size() == 2) {
            extraParamExpr = methodQueryExpr.getParameters().get(1);
        }
        else if (methodQueryExpr.getParameters().size() == 3) {
            queryFields = methodQueryExpr.getParameters().get(1);
            extraParamExpr = methodQueryExpr.getParameters().get(2);
        }


        String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();
        SimpleQueryStringBuilder simpleQueryString = QueryBuilders.simpleQueryStringQuery(text);

        if (queryFields != null) {
            String[] tArr = ElasticSqlArgTransferHelper.transferSqlArg(queryFields, sqlArgs, false).toString().split(COLON);
            if ("fields".equalsIgnoreCase(tArr[0])) {
                for (String fieldItem : tArr[1].split(COMMA)) {
                    simpleQueryString.field(fieldItem);
                }
            }
        }

        Map<String, String> extraParamMap = null;
        if (extraParamExpr != null) {
            String extraParam = ElasticSqlArgTransferHelper.transferSqlArg(extraParamExpr, sqlArgs, false).toString();
            extraParamMap = buildExtraMethodQueryParamsMap(extraParam);
        }

        setExtraMatchQueryParam(simpleQueryString, extraParamMap);

        return new AtomQuery(simpleQueryString);
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

        if (extraParamMap.containsKey("lowercase_expanded_terms")) {
            String val = extraParamMap.get("lowercase_expanded_terms");
            simpleStringQuery.lowercaseExpandedTerms(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            simpleStringQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey("analyze_wildcard")) {
            String val = extraParamMap.get("analyze_wildcard");
            simpleStringQuery.analyzeWildcard(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey("locale")) {
            String val = extraParamMap.get("locale");
            simpleStringQuery.locale(Locale.forLanguageTag(val));
        }

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
                simpleStringQuery.defaultOperator(SimpleQueryStringBuilder.Operator.AND);
            }
            if ("OR".equalsIgnoreCase(val)) {
                simpleStringQuery.defaultOperator(SimpleQueryStringBuilder.Operator.OR);
            }
        }
    }
}
