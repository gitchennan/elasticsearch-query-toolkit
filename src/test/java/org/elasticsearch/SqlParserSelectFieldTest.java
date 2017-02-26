package org.elasticsearch;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.junit.Assert;
import org.junit.Test;


public class SqlParserSelectFieldTest {

    @Test
    public void testParseFromSource() {
        String sql = "select id,productStatus from index.trx_order trx";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        Assert.assertEquals(parseResult.getIndex(), "index");
        Assert.assertEquals(parseResult.getType(), "trx_order");
        Assert.assertEquals(parseResult.getQueryAs(), "trx");
    }

    @Test
    public void testParseDefaultLimit() {
        String sql = "select id,productStatus from index.trx_order";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        Assert.assertEquals(parseResult.getFrom(), 0);
        Assert.assertEquals(parseResult.getSize(), 15);
    }

    @Test
    public void testParseOriSelectField() {
        String sql = "select id,productStatus from index.trx_order";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        Assert.assertTrue(parseResult.getQueryFieldList().size() == 2);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "id");
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "productStatus");
    }

    @Test
    public void testSelectAllField() {
        String sql = "select * from index.trx_order order by id asc routing by '801','802','803' limit 1,2";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toString());
        Assert.assertTrue(CollectionUtils.isEmpty(parseResult.getQueryFieldList()));
    }

    @Test
    public void testInnerDocField() {
        String sql = "select id,inner_doc(product.productStatus) from index.trx_order";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "id");
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "product.productStatus");

        sql = "select inner_doc(trx.product.productStatus) from index.trx_order trx";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "product.productStatus");

        sql = "select inner_doc(tmp.product.productStatus) from index.trx_order trx";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "tmp.product.productStatus");

        sql = "select product.productStatus from index.trx_order trx";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "product.productStatus");

        sql = "select id,product.* from index.trx_order trx";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "id");
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "product.*");
    }

    @Test
    public void testNestedDocField() {
        String sql = "select id,nested_doc(product.productStatus) from index.trx_order";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "id");
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "product.productStatus");

        sql = "select nested_doc(trx.product.productStatus) from index.trx_order trx";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "product.productStatus");

        sql = "select nested_doc(tmp.product.productStatus) from index.trx_order trx";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "tmp.product.productStatus");

        sql = "select product.productStatus from index.trx_order trx";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "product.productStatus");

        sql = "select id,product.* from index.trx_order trx";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "id");
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "product.*");
    }
}
