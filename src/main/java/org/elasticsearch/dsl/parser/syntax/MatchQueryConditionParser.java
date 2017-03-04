package org.elasticsearch.dsl.parser.syntax;

import org.elasticsearch.dsl.bean.ElasticDslContext;
import org.elasticsearch.dsl.bean.SQLCondition;
import org.elasticsearch.dsl.enums.SQLBoolOperator;
import org.elasticsearch.dsl.enums.SQLConditionType;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

public class MatchQueryConditionParser extends QueryWhereConditionParser {

    public MatchQueryConditionParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();

        if (queryBlock.getMatchQuery() != null) {
            SQLCondition matchQueryCondition = parseFilterCondition(dslContext, queryBlock.getMatchQuery());

            SQLBoolOperator operator = matchQueryCondition.getOperator();
            if(SQLConditionType.Atom == matchQueryCondition.getSQLConditionType()) {
                operator = SQLBoolOperator.AND;
            }

            BoolQueryBuilder boolQuery = mergeAtomFilter(matchQueryCondition.getFilterList(), operator);
            dslContext.getParseResult().setMatchCondition(boolQuery);
        }
    }
}
