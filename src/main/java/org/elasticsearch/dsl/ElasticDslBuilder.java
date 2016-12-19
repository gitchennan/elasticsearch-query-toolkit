package org.elasticsearch.dsl;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.*;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;

public class ElasticDslBuilder {

    private SQLQueryExpr queryExpr;

    public ElasticDslBuilder(SQLQueryExpr queryExpr) {
        this.queryExpr = queryExpr;
    }

    //SQL解析器的顺序不能改变
    private static final List<ElasticSqlParser> sqlParseProcessors = ImmutableList.of(
            //解析SQL指定的索引和文档类型
            QueryFromParser::parseQueryIndicesAndTypes,
            //解析SQL查询指定的where条件
            QueryWhereConditionParser::parseFilterCondition,
            //解析SQL排序条件
            QueryOrderConditionParser::parseOrderCondition,
            //解析SQL查询指定的字段
            QuerySelectFieldListParser::parseSelectFieldList

    );

    public ElasticDslContext build() {
        ElasticDslContext dslContext = new ElasticDslContext(queryExpr);
        if (queryExpr.getSubQuery().getQuery() instanceof ElasticSqlSelectQueryBlock) {
            sqlParseProcessors.stream().forEach(sqlParser -> sqlParser.parse(dslContext));
            return dslContext;
        }
        throw new ElasticSql2DslException("[syntax error] ElasticSql only support Select Sql, but get: " + queryExpr.getSubQuery().getQuery().getClass());
    }

}
