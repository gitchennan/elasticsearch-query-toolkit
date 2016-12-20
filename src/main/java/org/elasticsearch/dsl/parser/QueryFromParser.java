package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

public class QueryFromParser implements ElasticSqlParser {
    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        if (queryBlock.getFrom() instanceof SQLExprTableSource) {
            SQLExprTableSource tableSource = (SQLExprTableSource) queryBlock.getFrom();
            dslContext.setQueryAs(tableSource.getAlias());

            if (tableSource.getExpr() instanceof SQLIdentifierExpr) {
                dslContext.setIndices(ImmutableList.of(((SQLIdentifierExpr) tableSource.getExpr()).getName()));
                return;
            }
            if (tableSource.getExpr() instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr idxExpr = (SQLBinaryOpExpr) tableSource.getExpr();

                if (!(idxExpr.getLeft() instanceof SQLIdentifierExpr)
                        || !(idxExpr.getRight() instanceof SQLIdentifierExpr)
                        || idxExpr.getOperator() != SQLBinaryOperator.Divide) {
                    throw new ElasticSql2DslException("[syntax error] From table should like [index]/[type]");
                }
                dslContext.setIndices(ImmutableList.of(((SQLIdentifierExpr) idxExpr.getLeft()).getName()));
                dslContext.setTypes(ImmutableList.of(((SQLIdentifierExpr) idxExpr.getRight()).getName()));
                return;
            }
        }
        throw new ElasticSql2DslException("[syntax error] From table source should be [SQLExprTableSource],but get: " + queryBlock.getFrom().getClass());
    }
}
