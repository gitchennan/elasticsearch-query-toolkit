package org.es.sql.parser.query.method;


import org.es.sql.bean.AtomQuery;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.parser.query.method.expr.MethodExpression;

public interface MethodQueryParser extends MethodExpression {
    AtomQuery parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException;
}
