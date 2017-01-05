package org.elasticsearch;

import org.elasticsearch.dsl.ElasticSql2DslParser;
import org.elasticsearch.dsl.ElasticSqlParseResult;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.junit.Assert;
import org.junit.Test;


public class SqlParserWhereConditionTest  {
    @Test
    public void testParseEqExpr() {
        String sql = "select id,productStatus from index.trx_order trx where trx.status='SUCCESS'";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        FilterBuilder targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("status", "SUCCESS"));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where trx.price='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("price", "123.4"));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where inner_doc(product.price)='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("product.price", "123.4"));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where nested_doc(product.price)='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.nestedFilter("product", FilterBuilders.termFilter("price", "123.4")));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where nested_doc(trx.product.price)='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.nestedFilter("product", FilterBuilders.termFilter("price", "123.4")));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where nested_doc(abc.trx.product.price)='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.nestedFilter("abc.trx.product", FilterBuilders.termFilter("price", "123.4")));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where trx.product.price='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("product.price", "123.4"));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());
    }

    @Test
    public void testParseGtExpr() {

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();

        String sql = "select id,productStatus from index.trx_order trx where trx.price > 123.4";
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        FilterBuilder targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.rangeFilter("price").gt(123.4));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where inner_doc(product.price) > 123.4";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.rangeFilter("product.price").gt(123.4));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where nested_doc(product.price) > 123.4";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.nestedFilter("product", FilterBuilders.rangeFilter("price").gt(123.4)));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

    }

    @Test
    public void testParseDateExpr() {

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();

        String sql = "select id,productStatus from index.trx_order trx where trx.updatedAt > '2017-01-25'";
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        FilterBuilder targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.rangeFilter("updatedAt").gt("2017-01-25T00:00:00.000+0800"));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where trx.updatedAt > '2017-01-25 13:32'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.rangeFilter("updatedAt").gt("2017-01-25T13:32:00.000+0800"));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where trx.updatedAt > '2017-01-25 13:32:59'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.rangeFilter("updatedAt").gt("2017-01-25T13:32:59.000+0800"));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());

        sql = "select id,productStatus from index.trx_order trx where trx.updatedAt > date('yyyy/MM/dd hh:mm:ss', '2017/01/25 13:32:59')";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.rangeFilter("updatedAt").gt("2017-01-25T13:32:59.000+0800"));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());


        sql = "select id,productStatus from index.trx_order trx where trx.updatedAt > date('yyyy/MM/dd hh-mm', '2017/01/25 13-32')";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(FilterBuilders.rangeFilter("updatedAt").gt("2017-01-25T13:32:00.000+0800"));
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());


        sql = "select id,productStatus from index.trx_order trx where trx.updatedAt between date('yyyy/MM/dd hh-mm', '2017/01/25 13-32') and '2018-10-25'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = FilterBuilders.boolFilter().must(
                FilterBuilders.rangeFilter("updatedAt")
                        .gte("2017-01-25T13:32:00.000+0800")
                        .lte("2018-10-25T00:00:00.000+0800")
        );
        Assert.assertEquals(parseResult.getFilterBuilder().toString(), targetFilter.toString());
    }

    @Test
    public void testMixFilter() {
        String sql = "select p.displayName,p.id,p.code,p.trxPrice,p.investPeriod,p.investPeriodUnit,p.product.*"
                + " from trx_index.product_multiple_trx as p"
                + " where p.status = 'SUCCESS'"
                + " and p.price > 48000"
                + " and p.price < 60000"
                + " and inner_doc(p.product.productType) = 'TRANSFER_REQUEST'"
                + " and inner_doc(p.product.productCategory) in ('203', '204', '903')"
                + " and inner_doc(p.product.interestRate) >= 0.041"
                + " and inner_doc(p.product.investPeriodInDays) between 90 and 180"
                + " and p.updatedAt >= date('yyyy-MM-dd', '2016-01-01')"
                + " and p.updatedAt < '2017-01-01'"
                + " and (p.product.salesArea in ('2', '3', '4') or p.product.productCategory in ('901', '902'))"
                + " and (p.salesRegion is null or p.salesRegion = '2')"
                + " and nested_doc(p.productTags.type) in (?, ?)"
                + " order by nvl(p.price, 0) asc, nvl(inner_doc(p.product.interestRate), 0) desc";


        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new Object[]{"RM", "CX"});
        System.out.println(parseResult.toString());
    }
}
