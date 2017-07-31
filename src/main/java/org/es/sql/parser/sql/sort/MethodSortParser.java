package org.es.sql.parser.sql.sort;

import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.parser.query.method.MethodInvocation;
import org.es.sql.parser.query.method.expr.MethodExpression;


public interface MethodSortParser extends MethodExpression {
    SortBuilder parseMethodSortBuilder(MethodInvocation invocation, SortOrder order) throws ElasticSql2DslException;
}
