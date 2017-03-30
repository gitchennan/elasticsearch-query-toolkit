package org.elasticsearch.dsl.parser.query.method;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;

public abstract class CheckableMethodQueryParser implements MethodQueryParser {

    abstract void checkQueryMethod(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs);

    abstract AtomQuery parseMethodQueryExpr(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs);

    @Override
    public AtomQuery parseAtomMethodQuery(SQLMethodInvokeExpr methodExpr, String queryAs, Object[] sqlArgs) throws ElasticSql2DslException {
        checkQueryMethod(methodExpr, queryAs, sqlArgs);
        return parseMethodQueryExpr(methodExpr, queryAs, sqlArgs);
    }
}
