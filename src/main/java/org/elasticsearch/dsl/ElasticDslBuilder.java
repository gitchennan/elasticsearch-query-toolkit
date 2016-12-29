package org.elasticsearch.dsl;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.*;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;
import java.util.function.Consumer;

public class ElasticDslBuilder {

    private SQLQueryExpr queryExpr;

    public ElasticDslBuilder(SQLQueryExpr queryExpr) {
        this.queryExpr = queryExpr;
    }

    private List<ElasticSqlParser> buildSqlParserChain() {
        //SQL解析器的顺序不能改变
        return ImmutableList.of(
                //解析SQL指定的索引和文档类型
                new QueryFromParser(),
                //解析SQL查询指定的where条件
                new QueryWhereConditionParser(),
                //解析SQL排序条件
                new QueryOrderConditionParser(),
                //解析SQL查询指定的字段
                new QuerySelectFieldListParser(),
                //解析SQL的分页条数
                new QueryLimitSizeParser()
        );
    }

    public ElasticDslContext build() {
        final ElasticDslContext dslContext = new ElasticDslContext(queryExpr);
        if (queryExpr.getSubQuery().getQuery() instanceof ElasticSqlSelectQueryBlock) {
            buildSqlParserChain().stream().forEach(new Consumer<ElasticSqlParser>() {
                @Override
                public void accept(ElasticSqlParser sqlParser) {
                    sqlParser.parse(dslContext);
                }
            });
            return dslContext;
        }
        throw new ElasticSql2DslException("[syntax error] ElasticSql only support Select Sql, but get: " + queryExpr.getSubQuery().getQuery().getClass());
    }
}
