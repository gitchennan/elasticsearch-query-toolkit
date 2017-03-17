package org.elasticsearch.query;

import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.junit.Assert;
import org.junit.Test;


public class SqlParserRoutingTest {
    @Test
    public void testParseRoutingExpr() {
        String sql = "select * from index.order t where t.status='SUCCESS' order by id asc routing by 'A','B' limit 5,15";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());

        Assert.assertTrue("A".equalsIgnoreCase(parseResult.getRoutingBy().get(0)));
        Assert.assertTrue("B".equalsIgnoreCase(parseResult.getRoutingBy().get(1)));

        sql = "select * from index.order t where t.status='SUCCESS' order by id asc routing by 'A' limit 5,15";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        Assert.assertTrue("A".equalsIgnoreCase(parseResult.getRoutingBy().get(0)));

        System.out.println(parseResult.toDsl());
    }


    @Test
    public void testParseRoutingExprWithArgs() {
        String sql = "select * from index.order t where t.status='SUCCESS' order by id asc routing by ?,? limit 5,15";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new Object[]{"A", "B"});

        Assert.assertTrue("A".equalsIgnoreCase(parseResult.getRoutingBy().get(0)));
        Assert.assertTrue("B".equalsIgnoreCase(parseResult.getRoutingBy().get(1)));

        System.out.println(parseResult.toDsl());
    }
}
