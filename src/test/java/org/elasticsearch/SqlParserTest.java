package org.elasticsearch;

import org.elasticsearch.dsl.ElasticSql2DslParser;
import org.elasticsearch.dsl.ElasticSqlParseResult;
import org.junit.Test;


public class SqlParserTest {

    @Test
    public void testParseSelectFieldWithQueryAs() {
        String sql = "select id,productStatus from index.trx_order where "
                + " productCategory in ('801', '802') "
                + " and (updatedAt > date('yyyy-MM-dd hh:mm', ?) or updatedAt < '2017-01-01') "
                + " and price > ?";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new Object[]{"2016-12-30 14:58", 100.12});
        System.out.println(parseResult.toDsl());
    }
}
