package org.es.sql.parser.query.method.join;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.HasChildQueryBuilder;
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
 * has_child(childType, filterExpression, minChildren, maxChildren)
 * <p>
 * has_child('collection_plan', principal > 1000)
 * <p>
 * has_child('collection_plan', principal > 1000, 1, 4)
 *
 * @author chennan
 */
public class HasChildAtomQueryParser implements MethodQueryParser {

    private static List<String> HAS_CHILD_METHOD = ImmutableList.of("has_child", "hasChild", "has_child_query", "hasChildQuery");

    @Override
    public List<String> defineMethodNames() {
        return HAS_CHILD_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 2 || invocation.getParameterCount() != 4) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }
    }

    @Override
    public AtomQuery parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {
        String childType = invocation.getParameterAsString(0);
        SQLExpr filter = invocation.getParameter(1);

        BoolExpressionParser boolExpressionParser = new BoolExpressionParser();
        String queryAs = invocation.getQueryAs();
        SQLArgs sqlArgs = invocation.getSqlArgs();

        BoolQueryBuilder filterBuilder = boolExpressionParser.parseBoolQueryExpr(filter, queryAs, sqlArgs);
        HasChildQueryBuilder hasChildQueryBuilder = QueryBuilders.hasChildQuery(childType, filterBuilder, ScoreMode.None);

        if (invocation.getParameterCount() == 4) {
            Long minChildren = invocation.getParameterAsLong(2);
            Long maxChildren = invocation.getParameterAsLong(3);

            hasChildQueryBuilder.minMaxChildren(minChildren.intValue(), maxChildren.intValue());
        }

        return new AtomQuery(hasChildQueryBuilder);
    }
}
