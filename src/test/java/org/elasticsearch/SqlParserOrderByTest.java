package org.elasticsearch;

import org.elasticsearch.dsl.ElasticSql2DslParser;
import org.elasticsearch.dsl.ElasticSqlParseResult;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Assert;
import org.junit.Test;


public class SqlParserOrderByTest {
    @Test
    public void testParseEqExpr() {
        String sql = "select id,productStatus from index.trx_order order by price asc,id desc,updatedAt asc";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        SortBuilder targetSort = SortBuilders.fieldSort("price").order(SortOrder.ASC);
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());
        targetSort = SortBuilders.fieldSort("id").order(SortOrder.DESC);
        Assert.assertEquals(parseResult.getOrderBy().get(1).toString(), targetSort.toString());
        targetSort = SortBuilders.fieldSort("updatedAt").order(SortOrder.ASC);
        Assert.assertEquals(parseResult.getOrderBy().get(2).toString(), targetSort.toString());

        sql = "select id,productStatus from index.trx_order order by nvl(price, 0) asc";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        targetSort = SortBuilders.fieldSort("price").order(SortOrder.ASC).missing(0);
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());

        sql = "select id,productStatus from index.trx_order order by nvl(inner_doc(product.price), 0) asc";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        targetSort = SortBuilders.fieldSort("product.price").order(SortOrder.ASC).missing(0);
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());

        sql = "select id,productStatus from index.trx_order order by nvl(nested_doc(product.price), 0) asc";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        targetSort = SortBuilders.fieldSort("price").order(SortOrder.ASC).missing(0).setNestedPath("product");
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());


        sql = "select id,productStatus from index.trx_order order by nvl(product.price, 0) asc";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        targetSort = SortBuilders.fieldSort("product.price").order(SortOrder.ASC).missing(0);
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());

        sql = "select id,productStatus from index.trx_order order by product.price asc,nested_doc(productTags.sortNo) desc";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);
        targetSort = SortBuilders.fieldSort("product.price").order(SortOrder.ASC);
        Assert.assertEquals(parseResult.getOrderBy().get(0).toString(), targetSort.toString());
        targetSort = SortBuilders.fieldSort("sortNo").order(SortOrder.DESC).setNestedPath("productTags");
        Assert.assertEquals(parseResult.getOrderBy().get(1).toString(), targetSort.toString());
    }

}
