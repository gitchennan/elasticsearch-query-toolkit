package org.elasticsearch.dsl.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.helper.ElasticSqlMethodInvokeHelper;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.query.method.AbstractAtomMethodQueryParser;
import org.elasticsearch.dsl.parser.query.method.IConditionMethodQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;

import java.util.List;
import java.util.Map;

public class TermAtomQueryParser extends AbstractAtomMethodQueryParser {

    private static List<String> TERM_QUERY_METHOD = ImmutableList.of("term", "term_query", "termQuery");

    public TermAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public static Boolean isTermQuery(SQLMethodInvokeExpr methodQueryExpr) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(TERM_QUERY_METHOD, methodQueryExpr.getMethodName());
    }

    @Override
    protected void checkQueryMethod(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        if (Boolean.FALSE == isTermQuery(methodQueryExpr)) {
            throw new ElasticSql2DslException(String.format("[syntax error] Expected term query method name is [term],but get [%s]", methodQueryExpr.getMethodName()));
        }

        int paramCount = methodQueryExpr.getParameters().size();
        if (paramCount != 2 && paramCount != 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There's no %s args method: term", paramCount));
        }

        SQLExpr textExpr = methodQueryExpr.getParameters().get(1);

        String text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false).toString();
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Term text can not be blank!");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected AtomQuery parseMethodQueryExpr(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        SQLExpr queryField = methodQueryExpr.getParameters().get(0);
        SQLExpr textExpr = methodQueryExpr.getParameters().get(1);

        Map<String, String> extraParamMap = null;
        if (methodQueryExpr.getParameters().size() == 3) {
            SQLExpr ExtraParamExpr = methodQueryExpr.getParameters().get(2);
            String extraParam = ElasticSqlArgTransferHelper.transferSqlArg(ExtraParamExpr, sqlArgs, false).toString();

            extraParamMap = buildExtraMethodQueryParamsMap(extraParam);
        }

        Object text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false);

        return parseCondition(queryField, new Object[]{text, extraParamMap}, queryAs, new IConditionMethodQueryBuilder() {
            @Override
            public QueryBuilder buildQuery(String queryFieldName, Object[] parameters) {
                TermQueryBuilder termQuery = QueryBuilders.termQuery(queryFieldName, parameters[0].toString());

                if (parameters.length == 2 && parameters[1] != null) {
                    Map<String, String> tExtraParamMap = (Map<String, String>) parameters[1];
                    setExtraMatchQueryParam(termQuery, tExtraParamMap);
                }

                return termQuery;
            }
        });
    }

    private void setExtraMatchQueryParam(TermQueryBuilder termQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey("boost")) {
            String val = extraParamMap.get("boost");
            termQuery.boost(Float.valueOf(val));
        }
    }
}
