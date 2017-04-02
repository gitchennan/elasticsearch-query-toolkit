package org.es.sql.dsl.parser.sql;

import org.es.sql.druid.ElasticSqlSelectQueryBlock;
import org.es.sql.dsl.bean.ElasticDslContext;
import org.es.sql.dsl.listener.ParseActionListener;
import org.elasticsearch.index.query.BoolQueryBuilder;

public class QueryMatchConditionParser extends AbstractQueryConditionParser {

    public QueryMatchConditionParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();

        if (queryBlock.getMatchQuery() != null) {
            String queryAs = dslContext.getParseResult().getQueryAs();

            BoolQueryBuilder matchQuery = parseQueryConditionExpr(queryBlock.getMatchQuery(), queryAs, dslContext.getSqlArgs());

            dslContext.getParseResult().setMatchCondition(matchQuery);
        }
    }
}
