package org.elasticsearch.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLObjectImpl;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public class ElasticSqlSelectQueryBlock extends SQLSelectQueryBlock implements SQLObject {
    /*DSL: from to*/
    private Limit limit;


    public Limit getLimit() {
        return limit;
    }

    public void setLimit(Limit limit) {
        this.limit = limit;
    }

    public static class Limit extends SQLObjectImpl {
        public Limit() {

        }

        public Limit(SQLExpr rowCount) {
            this.setRowCount(rowCount);
        }

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
            throw new UnsupportedOperationException("ElasticSql un-support method : accept0(SQLASTVisitor visitor)");
        }

    }
}
