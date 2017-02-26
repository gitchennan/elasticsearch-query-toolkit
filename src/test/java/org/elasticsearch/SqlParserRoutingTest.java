package org.elasticsearch;

import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.junit.Assert;
import org.junit.Test;


public class SqlParserRoutingTest {
    @Test
    public void testParseRoutingExpr() {
        String sql = "select * from index.trx_order trx where trx.status='SUCCESS' order by id asc routing by '801','802' limit 5,15";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        Assert.assertTrue("801".equalsIgnoreCase(parseResult.getRoutingBy().get(0)));
        Assert.assertTrue("802".equalsIgnoreCase(parseResult.getRoutingBy().get(1)));

        sql = "select * from index.trx_order trx where trx.status='SUCCESS' order by id asc routing by '801' limit 5,15";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        Assert.assertTrue("801".equalsIgnoreCase(parseResult.getRoutingBy().get(0)));
    }

    @Test
    public void testParseRoutingExprWithArgs() {
        String sql = "select * from index.trx_order trx where trx.status='SUCCESS' order by id asc routing by ?,? limit 5,15";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new Object[]{"801", "802"});

        Assert.assertTrue("801".equalsIgnoreCase(parseResult.getRoutingBy().get(0)));
        Assert.assertTrue("802".equalsIgnoreCase(parseResult.getRoutingBy().get(1)));
    }
//
//    @Test
//    public void testJoin() {
//        String syntax = "select * from index.trx_order trx nested join product.xx x";
//        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
//        ElasticSqlParseResult parseResult = sql2DslParser.parse(syntax);
//    }

}
