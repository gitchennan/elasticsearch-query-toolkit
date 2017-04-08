package org.es;

import org.es.sql.dsl.bean.ElasticSqlParseResult;
import org.es.sql.dsl.parser.ElasticSql2DslParser;
import org.junit.Assert;
import org.junit.Test;


public class SqlParserLimitTest {
    @Test
    public void testParseLimitExpr() {
        String sql = "select id,status from index.order t where t.status='SUCCESS' limit 5,15";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        Assert.assertEquals(parseResult.getFrom(), 5);
        Assert.assertEquals(parseResult.getSize(), 15);
    }

    @Test
    public void testParseLimitExprWithArgs() {
        String sql = "select id,status from index.order t where t.status='SUCCESS' limit ?,?";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new Object[]{5, 15});

        Assert.assertEquals(parseResult.getFrom(), 5);
        Assert.assertEquals(parseResult.getSize(), 15);
    }
}
