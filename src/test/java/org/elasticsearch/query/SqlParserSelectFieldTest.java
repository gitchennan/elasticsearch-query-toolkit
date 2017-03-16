package org.elasticsearch.query;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.junit.Assert;
import org.junit.Test;


public class SqlParserSelectFieldTest {

    @Test
    public void testParseFromSource() {
        String sql = "select id,status from index.order t";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        Assert.assertEquals(parseResult.getIndices().get(0), "index");
        Assert.assertEquals(parseResult.getType(), "order");
        Assert.assertEquals(parseResult.getQueryAs(), "t");
        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseDefaultLimit() {
        String sql = "select id,status from index.order";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        Assert.assertEquals(parseResult.getFrom(), 0);
        Assert.assertEquals(parseResult.getSize(), 15);
        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseOriSelectField() {
        String sql = "select id,status from index.order t";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        Assert.assertTrue(parseResult.getQueryFieldList().size() == 2);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "id");
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "status");
        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testSelectAllField() {
        String sql = "select * from index.order order by id asc routing by 'A','B','C' limit 1,2";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        Assert.assertTrue(CollectionUtils.isEmpty(parseResult.getQueryFieldList()));
        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testInnerDocField() {
        String sql = "select id,product.status from index.order";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "id");
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "product.status");
        System.out.println(parseResult.toDsl());

        sql = "select id,product.status from index.order t";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "product.status");
        System.out.println(parseResult.toDsl());

        sql = "select tmp.product.status from index.order t";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "tmp.product.status");
        System.out.println(parseResult.toDsl());

        sql = "select product.status from index.order t";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "product.status");
        System.out.println(parseResult.toDsl());

        sql = "select id,product.* from index.order t";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "id");
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "product.*");
        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testNestedDocField() {
        String sql = "select id,$product.status from index.order";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "id");
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "product.status");
        System.out.println(parseResult.toDsl());


        sql = "select t.$product.status from index.order t";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "product.status");
        System.out.println(parseResult.toDsl());

        sql = "select $tmp.product.status from index.order t";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "tmp.product.status");
        System.out.println(parseResult.toDsl());


        sql = "select product.status from index.order t";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "product.status");
        System.out.println(parseResult.toDsl());

        sql = "select id,product.* from index.order t";
        parseResult = sql2DslParser.parse(sql);
        Assert.assertEquals(parseResult.getQueryFieldList().get(0), "id");
        Assert.assertEquals(parseResult.getQueryFieldList().get(1), "product.*");

        System.out.println(parseResult.toDsl());
    }
}
