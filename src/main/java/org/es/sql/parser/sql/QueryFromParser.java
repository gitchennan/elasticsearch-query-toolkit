package org.es.sql.parser.sql;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.google.common.collect.Lists;
import org.es.sql.bean.ElasticDslContext;
import org.es.sql.druid.ElasticSqlSelectQueryBlock;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.listener.ParseActionListener;


public class QueryFromParser implements QueryParser {

    private ParseActionListener parseActionListener;

    public QueryFromParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        if (queryBlock.getFrom() instanceof SQLExprTableSource) {
            SQLExprTableSource tableSource = (SQLExprTableSource) queryBlock.getFrom();
            dslContext.getParseResult().setQueryAs(tableSource.getAlias());

            if (tableSource.getExpr() instanceof SQLIdentifierExpr) {
                String index = ((SQLIdentifierExpr) tableSource.getExpr()).getName();
                dslContext.getParseResult().setIndices(Lists.newArrayList(index));
                return;
            }

            if (tableSource.getExpr() instanceof SQLPropertyExpr) {
                SQLPropertyExpr idxExpr = (SQLPropertyExpr) tableSource.getExpr();

                if (!(idxExpr.getOwner() instanceof SQLIdentifierExpr)) {
                    throw new ElasticSql2DslException("[syntax error] From table should like [index].[type]");
                }

                String index = ((SQLIdentifierExpr) idxExpr.getOwner()).getName();
                dslContext.getParseResult().setIndices(Lists.newArrayList(index));
                dslContext.getParseResult().setType(idxExpr.getName());
                return;
            }
        }
        throw new ElasticSql2DslException("[syntax error] From table should like [index].[type]");
    }
}
