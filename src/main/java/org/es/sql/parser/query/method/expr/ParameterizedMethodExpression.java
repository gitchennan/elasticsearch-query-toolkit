package org.es.sql.parser.query.method.expr;


import org.es.sql.parser.query.method.MethodInvocation;

import java.util.Map;

public interface ParameterizedMethodExpression extends MethodExpression {
    Map<String, String> generateParameterMap(MethodInvocation methodInvocation);
}
