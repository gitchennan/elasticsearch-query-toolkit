package org.es.sql.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.Token;
import com.google.common.collect.ImmutableList;
import org.es.sql.bean.ElasticDslContext;
import org.es.sql.bean.ElasticSqlParseResult;
import org.es.sql.bean.SQLArgs;
import org.es.sql.druid.ElasticSqlExprParser;
import org.es.sql.druid.ElasticSqlSelectQueryBlock;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.listener.ParseActionListener;
import org.es.sql.listener.ParseActionListenerAdapter;
import org.es.sql.parser.sql.*;


import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;

public class ElasticSql2DslParser {

    public ElasticSqlParseResult parse(String sql) throws ElasticSql2DslException {
        return parse(sql, null, new ParseActionListenerAdapter());
    }

    public ElasticSqlParseResult parse(String sql, ParseActionListener parseActionListener) throws ElasticSql2DslException {
        return parse(sql, null, parseActionListener);
    }

    public ElasticSqlParseResult parse(String sql, Object[] sqlArgs) throws ElasticSql2DslException {
        return parse(sql, sqlArgs, new ParseActionListenerAdapter());
    }

    public ElasticSqlParseResult parse(String sql, Object[] sqlArgs, ParseActionListener parseActionListener) throws ElasticSql2DslException {
        SQLQueryExpr queryExpr = null;
        try {
            ElasticSqlExprParser elasticSqlExprParser = new ElasticSqlExprParser(sql);
            SQLExpr sqlQueryExpr = elasticSqlExprParser.expr();
            check(elasticSqlExprParser, sqlQueryExpr, sqlArgs);
            queryExpr = (SQLQueryExpr) sqlQueryExpr;
        }
        catch (ParserException ex) {
            throw new ElasticSql2DslException(ex);
        }

        SQLArgs sqlParamValues = (sqlArgs != null && sqlArgs.length > 0) ? new SQLArgs(sqlArgs) : null;
        ElasticDslContext elasticDslContext = new ElasticDslContext(queryExpr, sqlParamValues);

        if (queryExpr.getSubQuery().getQuery() instanceof ElasticSqlSelectQueryBlock) {
            for (QueryParser sqlParser : buildSqlParserChain(parseActionListener)) {
                sqlParser.parse(elasticDslContext);
            }
        }
        else {
            throw new ElasticSql2DslException("[syntax error] Sql only support Select Sql");
        }
        return elasticDslContext.getParseResult();
    }

    private void check(ElasticSqlExprParser sqlExprParser, SQLExpr sqlQueryExpr, Object[] sqlArgs) {
        if (sqlExprParser.getLexer().token() != Token.EOF) {
            throw new ElasticSql2DslException("[syntax error] Sql last token is not EOF");
        }

        if (!(sqlQueryExpr instanceof SQLQueryExpr)) {
            throw new ElasticSql2DslException("[syntax error] Sql is not select druid");
        }

        if (sqlArgs != null && sqlArgs.length > 0) {
            for (Object arg : sqlArgs) {
                if (arg instanceof Array || arg instanceof Collection) {
                    throw new ElasticSql2DslException("[syntax error] Sql arg cannot support collection type");
                }
            }
        }
    }

    private List<QueryParser> buildSqlParserChain(ParseActionListener parseActionListener) {
        //SQL解析器的顺序不能改变
        return ImmutableList.of(
                //解析SQL指定的索引和文档类型
                new QueryFromParser(parseActionListener),
                //解析SQL查询指定的match条件
                new QueryMatchConditionParser(parseActionListener),
                //解析SQL查询指定的where条件
                new QueryWhereConditionParser(parseActionListener),
                //解析SQL排序条件
                new QueryOrderConditionParser(parseActionListener),
                //解析路由参数
                new QueryRoutingValParser(parseActionListener),
                //解析分组统计
                new QueryGroupByParser(parseActionListener),
                //解析SQL查询指定的字段
                new QuerySelectFieldListParser(parseActionListener),
                //解析SQL的分页条数
                new QueryLimitSizeParser(parseActionListener)
        );
    }
}
