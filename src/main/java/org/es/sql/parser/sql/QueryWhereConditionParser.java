package org.es.sql.parser.sql;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.es.sql.bean.ElasticDslContext;
import org.es.sql.druid.ElasticSqlSelectQueryBlock;
import org.es.sql.listener.ParseActionListener;

public class QueryWhereConditionParser extends AbstractQueryConditionParser {

    public QueryWhereConditionParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();

        if (queryBlock.getWhere() != null) {
            String queryAs = dslContext.getParseResult().getQueryAs();

            BoolQueryBuilder whereQuery = parseQueryConditionExpr(queryBlock.getWhere(), queryAs, dslContext.getSQLArgs());

            dslContext.getParseResult().setWhereCondition(whereQuery);
        }
    }
}
