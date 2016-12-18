package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

public class QueryIndexParser {
    public static void parseQueryIndicesAndTypes(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        if (queryBlock.getFrom() instanceof SQLExprTableSource) {
            SQLExprTableSource tableSource = (SQLExprTableSource) queryBlock.getFrom();

//            System.out.println(tableSource.getAlias());
//            System.out.println(tableSource.getExpr().getClass());

            dslContext.setIndices(ImmutableList.of(tableSource.getExpr().toString()));
        } else {
            throw new ElasticSql2DslException("[syntax error] From table source could be [SQLExprTableSource],but get: " + queryBlock.getFrom().getClass());
        }
    }
}
