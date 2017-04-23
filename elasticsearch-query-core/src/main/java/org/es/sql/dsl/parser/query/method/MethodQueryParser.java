package org.es.sql.dsl.parser.query.method;

import org.es.sql.dsl.bean.AtomQuery;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.parser.query.method.expr.MethodExpression;

public interface MethodQueryParser extends MethodExpression {
    AtomQuery parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException;
}
