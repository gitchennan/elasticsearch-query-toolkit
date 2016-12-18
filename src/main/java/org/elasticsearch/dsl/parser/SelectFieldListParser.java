package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.google.common.collect.Lists;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;
import org.elasticsearch.utils.StringUtils;

import java.util.List;

public class SelectFieldListParser {
    public static void parseSelectFieldList(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
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
