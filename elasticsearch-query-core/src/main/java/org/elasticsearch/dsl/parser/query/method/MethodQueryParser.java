package org.elasticsearch.dsl.parser.query.method;

import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;

import java.util.List;

public interface MethodQueryParser {

    List<String> defineMethodNames();

    boolean isMatchMethodInvocation(MethodInvocation invocation);

    AtomQuery parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException;
}
