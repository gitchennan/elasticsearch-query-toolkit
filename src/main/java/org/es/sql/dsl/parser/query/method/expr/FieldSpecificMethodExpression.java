package org.es.sql.dsl.parser.query.method.expr;

import com.alibaba.druid.sql.ast.SQLExpr;
import org.es.sql.dsl.parser.query.method.MethodInvocation;

public interface FieldSpecificMethodExpression extends MethodExpression {
    SQLExpr defineFieldExpr(MethodInvocation invocation);
}
