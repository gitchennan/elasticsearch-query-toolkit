package org.elasticsearch.dsl.parser.syntax.sql;

import org.elasticsearch.dsl.bean.ElasticDslContext;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

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
