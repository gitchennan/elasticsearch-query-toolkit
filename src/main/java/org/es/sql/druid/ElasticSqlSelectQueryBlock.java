package org.es.sql.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLObjectImpl;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class ElasticSqlSelectQueryBlock extends SQLSelectQueryBlock implements SQLObject {
    //DSL: from to
    private Limit limit;

    private Routing routing;

    private SQLExpr matchQuery;

    public SQLExpr getMatchQuery() {
        return matchQuery;
    }

    public void setMatchQuery(SQLExpr matchQuery) {
        this.matchQuery = matchQuery;
    }

    public Limit getLimit() {
        return limit;
    }

    public void setLimit(Limit limit) {
        this.limit = limit;
    }

    public Routing getRouting() {
        return routing;
    }

    public void setRouting(Routing routing) {
        this.routing = routing;
    }

    public static class Routing extends SQLObjectImpl {
        private List<SQLExpr> routingValues;

        public Routing(List<SQLExpr> routingValues) {
            this.routingValues = routingValues;
            if (CollectionUtils.isNotEmpty(routingValues)) {
                for (SQLExpr sqlExpr : routingValues) {
                    sqlExpr.setParent(ElasticSqlSelectQueryBlock.Routing.this);
                }
            }
        }

        @Override
        protected void accept0(SQLASTVisitor visitor) {
            throw new UnsupportedOperationException("accept0(SQLASTVisitor visitor)");
        }

        public List<SQLExpr> getRoutingValues() {
            return routingValues;
        }
    }

    public static class Limit extends SQLObjectImpl {
        private SQLExpr rowCount;
        private SQLExpr offset;

        public SQLExpr getRowCount() {
            return rowCount;
        }

        public void setRowCount(SQLExpr rowCount) {
            if (rowCount != null) {
                rowCount.setParent(this);
            }
            this.rowCount = rowCount;
        }

        public SQLExpr getOffset() {
            return offset;
        }

        public void setOffset(SQLExpr offset) {
            if (offset != null) {
                offset.setParent(this);
            }
            this.offset = offset;
        }

        @Override
        protected void accept0(SQLASTVisitor visitor) {
            throw new UnsupportedOperationException("accept0(SQLASTVisitor visitor)");
        }

    }
}
