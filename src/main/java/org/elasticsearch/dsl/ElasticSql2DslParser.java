package org.elasticsearch.dsl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.Token;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.*;
import org.elasticsearch.sql.ElasticSqlExprParser;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public class ElasticSql2DslParser {

    /**
     * 执行sql到dsl转换
     *
     * @param sql 待解析sql
     * @return 解析结果
     * @throws ElasticSql2DslException 当解析出现语法错误时抛出
     */
    public ElasticSqlParseResult parse(String sql) throws ElasticSql2DslException {
        return parse(sql, null);
    }


    /**
     * 执行sql到dsl转换
     *
     * @param sql     待解析sql
     * @param sqlArgs sql动态参数
     * @return 解析结果
     * @throws ElasticSql2DslException 当解析出现语法错误时抛出
     */
    public ElasticSqlParseResult parse(String sql, Object[] sqlArgs) throws ElasticSql2DslException {
        SQLQueryExpr queryExpr = null;
        try {
            ElasticSqlExprParser elasticSqlExprParser = new ElasticSqlExprParser(sql);
            SQLExpr sqlQueryExpr = elasticSqlExprParser.expr();
            check(elasticSqlExprParser, sqlQueryExpr, sqlArgs);
            queryExpr = (SQLQueryExpr) sqlQueryExpr;
        } catch (ParserException ex) {
            throw new ElasticSql2DslException(ex);
        }

        final ElasticDslContext elasticDslContext = new ElasticDslContext(queryExpr, sqlArgs);
        if (queryExpr.getSubQuery().getQuery() instanceof ElasticSqlSelectQueryBlock) {
            buildSqlParserChain().stream().forEach(new Consumer<QueryParser>() {
                @Override
                public void accept(QueryParser sqlParser) {
                    sqlParser.parse(elasticDslContext);
                }
            });
        } else {
            throw new ElasticSql2DslException("[syntax error] ElasticSql only support Select Sql");
        }
        return elasticDslContext.getParseResult();
    }

    private void check(ElasticSqlExprParser sqlExprParser, SQLExpr sqlQueryExpr, Object[] sqlArgs) {
        if (sqlExprParser.getLexer().token() != Token.EOF) {
            throw new ElasticSql2DslException("[syntax error] Sql last token is not EOF");
        }

        if (!(sqlQueryExpr instanceof SQLQueryExpr)) {
            throw new ElasticSql2DslException("[syntax error] Sql is not select sql");
        }

        if (sqlArgs != null && sqlArgs.length > 0) {
            for (Object arg : sqlArgs) {
                if (arg instanceof Array || arg instanceof Collection) {
                    throw new ElasticSql2DslException("[syntax error] Sql arg cannot support collection type");
                }
            }
        }
    }

    private List<QueryParser> buildSqlParserChain() {
        //SQL解析器的顺序不能改变
        return ImmutableList.of(
                //解析SQL指定的索引和文档类型
                new QueryFromParser(),
                //解析SQL查询指定的where条件
                new QueryWhereConditionParser(),
                //解析SQL排序条件
                new QueryOrderConditionParser(),
                //解析路由参数
                new QueryRoutingValParser(),
                //解析SQL查询指定的字段
                new QuerySelectFieldListParser(),
                //解析SQL的分页条数
                new QueryLimitSizeParser()
        );
    }
}
