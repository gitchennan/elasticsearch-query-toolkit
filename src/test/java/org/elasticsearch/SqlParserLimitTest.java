package org.elasticsearch;

import org.elasticsearch.dsl.*;
import org.junit.Assert;
import org.junit.Test;


public class SqlParserLimitTest {
    @Test
    public void testParseLimitExpr() {
        String sql = "select id,productStatus from index.trx_order trx where trx.status='SUCCESS' limit 5,15";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        Assert.assertEquals(parseResult.getFrom(), 5);
        Assert.assertEquals(parseResult.getSize(), 15);
    }

    @Test
    public void testParseLimitExprWithArgs() {
        String sql = "select id,productStatus from index.trx_order trx where trx.status='SUCCESS' limit ?,?";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new Object[]{5, 15});

        Assert.assertEquals(parseResult.getFrom(), 5);
        Assert.assertEquals(parseResult.getSize(), 15);
    }

    @Test
    public void testParseLimitExprWithListener() {
        String sql = "select id,productStatus from index.trx_order trx where trx.status = 'SUCCESS' limit 5,15";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new ParseActionListenerAdapter() {
            @Override
            public void onAtomConditionParse(ElasticSqlIdentifier sqlIdentifier, Object[] paramValues, SQLConditionOperator operator) {
                System.out.println(sqlIdentifier.getPathName());
                System.out.println(sqlIdentifier.getPropertyName());
                System.out.println(sqlIdentifier.getIdentifierType());
            }

            @Override
            public void onSqlSelectFieldParse(ElasticSqlIdentifier sqlIdentifier) {
                System.out.println(sqlIdentifier.getPropertyName());
            }
        });

        Assert.assertEquals(parseResult.getFrom(), 5);
        Assert.assertEquals(parseResult.getSize(), 15);
    }


}
