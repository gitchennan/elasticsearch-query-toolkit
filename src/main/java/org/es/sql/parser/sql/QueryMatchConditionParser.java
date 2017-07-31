package org.es.sql.parser.sql;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.es.sql.bean.ElasticDslContext;
import org.es.sql.druid.ElasticSqlSelectQueryBlock;
import org.es.sql.listener.ParseActionListener;

public class QueryMatchConditionParser extends BoolExpressionParser implements QueryParser{

    public QueryMatchConditionParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();

        if (queryBlock.getMatchQuery() != null) {
            String queryAs = dslContext.getParseResult().getQueryAs();

            BoolQueryBuilder matchQuery = parseBoolQueryExpr(queryBlock.getMatchQuery(), queryAs, dslContext.getSQLArgs());

            dslContext.getParseResult().setMatchCondition(matchQuery);
        }
    }
}
