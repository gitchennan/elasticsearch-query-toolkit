package org.elasticsearch;

import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Assert;
import org.junit.Test;


public class SqlParserOrderByTest {
    @Test
    public void testParseEqExpr() {
        String sql = "select id,status from index.order order by price asc,id desc,lastUpdateTime asc";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        SortBuilder targetSort = SortBuilders.fieldSort("price").order(SortOrder.ASC);
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());
        targetSort = SortBuilders.fieldSort("id").order(SortOrder.DESC);
        Assert.assertEquals(parseResult.getOrderBy().get(1).toString(), targetSort.toString());
        targetSort = SortBuilders.fieldSort("lastUpdateTime").order(SortOrder.ASC);
        Assert.assertEquals(parseResult.getOrderBy().get(2).toString(), targetSort.toString());

        sql = "select id,status from index.order order by nvl(price, 0) asc";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        targetSort = SortBuilders.fieldSort("price").order(SortOrder.ASC).missing(0);
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());

        sql = "select id,status from index.order order by nvl(product.price, 0) asc";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        targetSort = SortBuilders.fieldSort("product.price").order(SortOrder.ASC).missing(0);
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());

        sql = "select id,status from index.order order by nvl($product.price, 0) asc";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        targetSort = SortBuilders.fieldSort("product.price").order(SortOrder.ASC).missing(0).setNestedPath("product");
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());


        sql = "select id,status from index.order order by nvl(product.price, 0) asc";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        targetSort = SortBuilders.fieldSort("product.price").order(SortOrder.ASC).missing(0);
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());

        sql = "select id,status from index.order order by product.price asc, $productTags.sortNo desc";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        targetSort = SortBuilders.fieldSort("product.price").order(SortOrder.ASC);
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());
        targetSort = SortBuilders.fieldSort("productTags.sortNo").order(SortOrder.DESC).setNestedPath("productTags");
        Assert.assertEquals(parseResult.getOrderBy().get(1).toString(), targetSort.toString());
    }

    @Test
    public void testX() {
        String sql = "select * from index.order where (a=0 and d=0) or (b=0 and c=0)";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        System.out.println(sql2DslParser.parse(sql).toDsl());
    }

}
