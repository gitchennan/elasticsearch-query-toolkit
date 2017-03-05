package org.elasticsearch.dsl.parser.syntax.query.fulltext;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.parser.helper.ElasticSqlArgTransferHelper;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Map;

public class MatchAtomQueryParser extends AbstractFullTextQueryParser {

    public MatchAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    protected void checkMatchQueryMethod(SQLMethodInvokeExpr matchQueryExpr) {

    }

    @Override
    protected AtomQuery parseMatchQueryMethodExpr(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs) {
        SQLExpr queryField = matchQueryExpr.getParameters().get(0);
        SQLExpr textExpr = matchQueryExpr.getParameters().get(1);

        Map<String, String> extraParamMap = Maps.newHashMap();
        if (matchQueryExpr.getParameters().size() == 3) {
            SQLExpr ExtraParamExpr = matchQueryExpr.getParameters().get(2);
            String extraParam = ElasticSqlArgTransferHelper.transferSqlArg(ExtraParamExpr, sqlArgs, false).toString();

            for (String paramPair : extraParam.split(COMMA)) {
                String[] paramPairArr = paramPair.split(COLON);
                extraParamMap.put(paramPairArr[0], paramPairArr[1]);
            }
        }

        Object text = ElasticSqlArgTransferHelper.transferSqlArg(textExpr, sqlArgs, false);

        return parseCondition(queryField, new Object[]{text}, queryAs, new IConditionFullTextQueryBuilder() {
            @Override
            public QueryBuilder buildQuery(String queryFieldName, Object[] parameters) {
                MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(queryFieldName, parameters[0]);
                if (MapUtils.isNotEmpty(extraParamMap)) {
                    setExtraMatchQueryParam(matchQuery, extraParamMap);
                }
                return matchQuery;
            }
        });
    }

    private void setExtraMatchQueryParam(MatchQueryBuilder matchQuery, Map<String, String> extraParamMap) {
        if (extraParamMap.containsKey("operator")) {
            String val = extraParamMap.get("operator");

            if ("and".equalsIgnoreCase(val)) {
                matchQuery.operator(MatchQueryBuilder.Operator.AND);
            }

            if ("or".equalsIgnoreCase(val)) {
                matchQuery.operator(MatchQueryBuilder.Operator.OR);
            }
        }

        if (extraParamMap.containsKey("minimum_should_match")) {
            String val = extraParamMap.get("minimum_should_match");

            matchQuery.minimumShouldMatch(val);
        }
    }
}
