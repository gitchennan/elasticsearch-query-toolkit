package org.es.sql.parser.query.method.join;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.HasParentQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import org.es.sql.bean.AtomQuery;
import org.es.sql.bean.SQLArgs;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.helper.ElasticSqlMethodInvokeHelper;
import org.es.sql.parser.query.method.MethodInvocation;
import org.es.sql.parser.query.method.MethodQueryParser;
import org.es.sql.parser.sql.BoolExpressionParser;

import java.util.List;

/**
 * has_parent(parentType, filterExpression)
 * <p>
 * has_parent('investment', principal > 100 and status='SUCCESS')
 *
 * @author chennan
 */
public class HasParentAtomQueryParser implements MethodQueryParser {

    private static List<String> HAS_PARENT_METHOD = ImmutableList.of("has_parent", "hasParent", "has_parent_query", "hasParentQuery");

    @Override
    public List<String> defineMethodNames() {
        return HAS_PARENT_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 2) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }
    }

    @Override
    public AtomQuery parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {
        String parentType = invocation.getParameterAsString(0);
        SQLExpr filter = invocation.getParameter(1);

        BoolExpressionParser boolExpressionParser = new BoolExpressionParser();
        String queryAs = invocation.getQueryAs();
        SQLArgs sqlArgs = invocation.getSqlArgs();

        BoolQueryBuilder filterBuilder = boolExpressionParser.parseBoolQueryExpr(filter, queryAs, sqlArgs);
        HasParentQueryBuilder hasParentQueryBuilder = QueryBuilders.hasParentQuery(parentType, filterBuilder, false);

        return new AtomQuery(hasParentQueryBuilder);
    }
}
