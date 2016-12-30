package org.elasticsearch.dsl.parser.helper;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.utils.StringUtils;

public class ElasticSqlIdentifierHelper {

    public static void parseSqlIdentifier(SQLExpr idfSqlExpr, String queryAsAlias, ElasticSqlTopIdfFunc topIdfFunc, ElasticSqlNestIdfFunc nestIdfFunc) {
        if (idfSqlExpr instanceof SQLIdentifierExpr) {
            topIdfFunc.parse(((SQLIdentifierExpr) idfSqlExpr).getName());
        }
        if (idfSqlExpr instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) idfSqlExpr;

            if (propertyExpr.getOwner() instanceof SQLPropertyExpr) {
                SQLPropertyExpr ownerPropertyExpr = (SQLPropertyExpr) propertyExpr.getOwner();
                if (!(ownerPropertyExpr.getOwner() instanceof SQLIdentifierExpr)) {
                    throw new ElasticSql2DslException("[syntax error] Select field ref level should <= 3");
                }
                SQLIdentifierExpr superOwnerIdfExpr = (SQLIdentifierExpr) ownerPropertyExpr.getOwner();
                if (StringUtils.isNotBlank(queryAsAlias) && queryAsAlias.equalsIgnoreCase(superOwnerIdfExpr.getName())) {
                    nestIdfFunc.parse(ownerPropertyExpr.getName(), propertyExpr.getName());
                } else {
                    throw new ElasticSql2DslException("[syntax error] Select field qualifier not support: " + superOwnerIdfExpr.getName());
                }
            } else if (propertyExpr.getOwner() instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr ownerIdfExpr = (SQLIdentifierExpr) propertyExpr.getOwner();
                if (StringUtils.isNotBlank(queryAsAlias) && queryAsAlias.equalsIgnoreCase(ownerIdfExpr.getName())) {
                    topIdfFunc.parse(propertyExpr.getName());
                } else {
                    nestIdfFunc.parse(ownerIdfExpr.getName(), propertyExpr.getName());
                }
            }
        }
    }

    @FunctionalInterface
    public interface ElasticSqlTopIdfFunc {
        void parse(String idfName);
    }

    @FunctionalInterface
    public interface ElasticSqlNestIdfFunc {
        void parse(String nestPath, String idfName);
    }

}
