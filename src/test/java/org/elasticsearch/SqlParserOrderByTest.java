package org.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.util.ElasticMockClient;
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
        ElasticMockClient esClient = new ElasticMockClient();

        String sql = "select * from index.order where status='SUCCESS' order by nvl(pride, 0) asc routing by 'CA','CB' limit 0, 20";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        //解析SQL
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        //生成DSL,可用于rest api调用
        String dsl = parseResult.toDsl();

        //toRequest方法接收一个clinet对象参数，用于生成SearchRequestBuilder
        SearchRequestBuilder searchReq = parseResult.toRequest(esClient);

        //执行查询
        //SearchResponse response = searchReq.execute().actionGet();

        System.out.println(dsl);
    }

}
