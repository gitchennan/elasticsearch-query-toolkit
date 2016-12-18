package org.elasticsearch.dsl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.google.common.collect.Lists;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;
import org.elasticsearch.utils.StringUtils;

import java.util.List;

public class ElasticDslBuilder {

    private SQLQueryExpr queryExpr;

    public ElasticDslBuilder(SQLQueryExpr queryExpr) {
        this.queryExpr = queryExpr;
    }

    public ElasticDslContext build() {
        ElasticDslContext dslContext = new ElasticDslContext();
        if (queryExpr.getSubQuery().getQuery() instanceof ElasticSqlSelectQueryBlock) {
            ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) queryExpr.getSubQuery().getQuery();
            //解析SQL指定索引别名
            parseQueryAs(dslContext, queryBlock);
            //解析SQL查询条件
            parseFilterCondition(dslContext.boolFilter(), queryBlock.getWhere());
            //解析查询阶段
            parseSelectFieldList(dslContext, queryBlock);
        }
        return dslContext;
    }

    private void parseFilterCondition(BoolFilterBuilder filterBuilder, SQLExpr sqlExpr) {
        System.out.println(sqlExpr.getClass());
        if (sqlExpr == null) {
            return;
        }

        //throw new ElasticSql2DslException("[syntax error] Where condition not support type: " + sqlExpr.getClass());

        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinOpExpr = (SQLBinaryOpExpr) sqlExpr;
        }
    }

    private void parseQueryAs(ElasticDslContext dslContext, ElasticSqlSelectQueryBlock queryBlock) {
        dslContext.setQueryAs(queryBlock.getFrom().getAlias());
    }

    private void parseSelectFieldList(ElasticDslContext dslContext, ElasticSqlSelectQueryBlock queryBlock) {
        List<String> selectFields = Lists.newLinkedList();
        String queryAsAlias = dslContext.getQueryAs();

        if (queryBlock.getSelectList().size() == 1 && (queryBlock.getSelectList().get(0).getExpr() instanceof SQLIdentifierExpr)) {
            SQLIdentifierExpr idfExpr = (SQLIdentifierExpr) queryBlock.getSelectList().get(0).getExpr();
            if ("*".equalsIgnoreCase(idfExpr.getName())) {
                return;
            }
        }

        queryBlock.getSelectList().stream().forEach(selectFieldItem -> {
            if (selectFieldItem.getExpr() instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr idfExpr = (SQLIdentifierExpr) selectFieldItem.getExpr();
                selectFields.add(idfExpr.getName());
            }
            if (selectFieldItem.getExpr() instanceof SQLPropertyExpr) {
                SQLPropertyExpr propertyExpr = (SQLPropertyExpr) selectFieldItem.getExpr();

                if (propertyExpr.getOwner() instanceof SQLPropertyExpr) {
                    SQLPropertyExpr ownerPropertyExpr = (SQLPropertyExpr) propertyExpr.getOwner();
                    if (!(ownerPropertyExpr.getOwner() instanceof SQLIdentifierExpr)) {
                        throw new ElasticSql2DslException("[syntax error] Select field ref level could <= 3");
                    }
                    SQLIdentifierExpr superOwnerIdfExpr = (SQLIdentifierExpr) ownerPropertyExpr.getOwner();
                    if (StringUtils.isNotBlank(queryAsAlias) && queryAsAlias.equalsIgnoreCase(superOwnerIdfExpr.getName())) {
                        selectFields.add(String.format("%s.%s", ownerPropertyExpr.getName(), propertyExpr.getName()));
                    } else {
                        throw new ElasticSql2DslException("[syntax error] Select field qualifier not support: " + superOwnerIdfExpr.getName());
                    }
                } else if (propertyExpr.getOwner() instanceof SQLIdentifierExpr) {
                    SQLIdentifierExpr ownerIdfExpr = (SQLIdentifierExpr) propertyExpr.getOwner();
                    if (StringUtils.isNotBlank(queryAsAlias) && queryAsAlias.equalsIgnoreCase(ownerIdfExpr.getName())) {
                        selectFields.add(propertyExpr.getName());
                    } else {
                        selectFields.add(String.format("%s.%s", ownerIdfExpr.getName(), propertyExpr.getName()));
                    }
                }
            }
        });
        dslContext.setQueryFieldList(selectFields);
    }

}
