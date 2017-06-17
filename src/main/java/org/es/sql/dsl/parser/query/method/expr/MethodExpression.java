package org.es.sql.dsl.parser.query.method.expr;

import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.parser.query.method.MethodInvocation;

import java.util.List;

public interface MethodExpression {
    List<String> defineMethodNames();

    boolean isMatchMethodInvocation(MethodInvocation invocation);

    void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException;
}
