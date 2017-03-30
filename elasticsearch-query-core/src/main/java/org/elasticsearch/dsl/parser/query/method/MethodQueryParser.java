package org.elasticsearch.dsl.parser.query.method;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;

public interface MethodQueryParser {

    boolean isMethodOf(SQLMethodInvokeExpr methodExpr);

    AtomQuery parseAtomMethodQuery(SQLMethodInvokeExpr methodExpr, String queryAs, Object[] sqlArgs) throws ElasticSql2DslException;
}
