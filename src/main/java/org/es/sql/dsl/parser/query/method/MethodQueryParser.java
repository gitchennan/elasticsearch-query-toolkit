package org.es.sql.dsl.parser.query.method;

import org.es.sql.dsl.bean.AtomFilter;
import org.es.sql.dsl.exception.ElasticSql2DslException;

import java.util.List;

public interface MethodQueryParser {

    List<String> defineMethodNames();

    boolean isMatchMethodInvocation(MethodInvocation invocation);

    AtomFilter parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException;
}
