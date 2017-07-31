package org.es.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.es.sql.bean.AtomQuery;
import org.es.sql.bean.SQLArgs;
import org.es.sql.enums.SQLConditionOperator;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.helper.ElasticSqlArgConverter;
import org.es.sql.listener.ParseActionListener;


public class InListAtomQueryParser extends AbstractAtomExactQueryParser {

    public InListAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public AtomQuery parseInListQuery(SQLInListExpr inListQueryExpr, String queryAs, SQLArgs SQLArgs) {
        if (CollectionUtils.isEmpty(inListQueryExpr.getTargetList())) {
            throw new ElasticSql2DslException("[syntax error] In list expr target list cannot be blank");
        }

        Object[] targetInList = ElasticSqlArgConverter.convertSqlArgs(inListQueryExpr.getTargetList(), SQLArgs);
        SQLConditionOperator operator = inListQueryExpr.isNot() ? SQLConditionOperator.NotIn : SQLConditionOperator.In;

        return parseCondition(inListQueryExpr.getExpr(), operator, targetInList, queryAs, new IConditionExactQueryBuilder() {
            @Override
            public QueryBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues) {
                if (SQLConditionOperator.NotIn == operator) {
                    return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(queryFieldName, rightParamValues));
                }
                else {
                    return QueryBuilders.termsQuery(queryFieldName, rightParamValues);
                }
            }
        });
    }
}
