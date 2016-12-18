package org.elasticsearch.dsl.parser;

import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

public class QueryAsAliasParser {
    public static void parseQueryAsAlias(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        dslContext.setQueryAs(queryBlock.getFrom().getAlias());
    }
}
