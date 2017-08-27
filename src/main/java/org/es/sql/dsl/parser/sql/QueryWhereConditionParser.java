package org.es.sql.dsl.parser.sql;

import org.es.sql.druid.ElasticSqlSelectQueryBlock;
import org.es.sql.dsl.bean.ElasticDslContext;
import org.es.sql.dsl.listener.ParseActionListener;
import org.elasticsearch.index.query.BoolQueryBuilder;

public class QueryWhereConditionParser extends AbstractQueryConditionParser {

    public QueryWhereConditionParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();

        if (queryBlock.getWhere() != null) {
            String queryAs = dslContext.getParseResult().getQueryAs();

            BoolQueryBuilder whereQuery = parseQueryConditionExpr(queryBlock.getWhere(), queryAs, dslContext.getSqlArgs());

            dslContext.getParseResult().setWhereCondition(whereQuery);
        }
    }
}
