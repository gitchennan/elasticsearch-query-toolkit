package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

public class QueryLimitSizeParser implements ElasticSqlParser {
    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        if (queryBlock.getLimit() != null) {
            if (!(queryBlock.getLimit().getOffset() instanceof SQLIntegerExpr)) {
                throw new ElasticSql2DslException("[syntax error] Sql limit expr offset should be a non-negative number");
            }
            dslContext.setFrom(((SQLIntegerExpr) queryBlock.getLimit().getOffset()).getNumber().intValue());

            if (!(queryBlock.getLimit().getRowCount() instanceof SQLIntegerExpr)) {
                throw new ElasticSql2DslException("[syntax error] Sql limit expr row count should be a non-negative number");
            }
            dslContext.setSize(((SQLIntegerExpr) queryBlock.getLimit().getRowCount()).getNumber().intValue());
        } else {
            dslContext.setFrom(0);
            dslContext.setSize(15);
        }
    }
}
