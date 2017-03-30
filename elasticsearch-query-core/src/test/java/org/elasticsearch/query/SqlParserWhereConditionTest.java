package org.elasticsearch.query;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.utils.ElasticMockClient;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class SqlParserWhereConditionTest {
    @Test
    public void testParseEqExpr() {
        String sql = "select id,status from index.order t where t.status='SUCCESS'";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        QueryBuilder targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("status", "SUCCESS"));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where t.price='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("price", "123.4"));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where product.price='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("product.price", "123.4"));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where $product.price='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.nestedQuery("product", QueryBuilders.termQuery("product.price", "123.4")));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where t.$product.price='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.nestedQuery("product", QueryBuilders.termQuery("product.price", "123.4")));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where abc.t.$product.price='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.nestedQuery("abc.t.product", QueryBuilders.termQuery("abc.t.product.price", "123.4")));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where t.product.price='123.4'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("product.price", "123.4"));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseGtExpr() {

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();

        String sql = "select id,status from index.order t where t.price > 123.4";
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        QueryBuilder targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("price").gt(123.4));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where product.price > 123.4";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("product.price").gt(123.4));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where $product.price > 123.4";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.nestedQuery("product", QueryBuilders.rangeQuery("product.price").gt(123.4)));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseDateExpr() {

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();

        String sql = "select id,status from index.order t where t.lastUpdateTime > '2017-01-25'";
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        QueryBuilder targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("lastUpdateTime").gt("2017-01-25T00:00:00.000+0800"));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where t.lastUpdateTime > '2017-01-25 13:32'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("lastUpdateTime").gt("2017-01-25T13:32:00.000+0800"));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where t.lastUpdateTime > '2017-01-25 13:32:59'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("lastUpdateTime").gt("2017-01-25T13:32:59.000+0800"));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where t.lastUpdateTime > to_date('yyyy/MM/dd hh:mm:ss', '2017/01/25 13:32:59')";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("lastUpdateTime").gt("2017-01-25T13:32:59.000+0800"));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where t.lastUpdateTime > to_date('yyyy/MM/dd hh-mm', '2017/01/25 13-32')";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("lastUpdateTime").gt("2017-01-25T13:32:00.000+0800"));
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());

        sql = "select id,status from index.order t where t.lastUpdateTime between to_date('yyyy/MM/dd hh-mm', '2017/01/25 13-32') and '2018-10-25'";
        parseResult = sql2DslParser.parse(sql);
        targetFilter = QueryBuilders.boolQuery().must(
                QueryBuilders.rangeQuery("lastUpdateTime")
                        .gte("2017-01-25T13:32:00.000+0800")
                        .lte("2018-10-25T00:00:00.000+0800")
        );
        Assert.assertEquals(parseResult.getWhereCondition().toString(), targetFilter.toString());
        System.out.println(parseResult.toDsl());
    }


    @Test
    public void test$Expr() {
        String sql = "select * from index where a.$b.c.$d.e > 2";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testCreateSearchDsl() {
        SearchRequestBuilder searchReq = new SearchRequestBuilder(new ElasticMockClient(), SearchAction.INSTANCE);

        NestedQueryBuilder categoryNameTerm = QueryBuilders.nestedQuery("bookCategories", QueryBuilders.termQuery("bookCategories.categoryName", "ART"));
        NestedQueryBuilder bookAuthorNestedFilter = QueryBuilders.nestedQuery("bookCategories.books", QueryBuilders.termQuery("bookCategories.books.bookAuthor", "bibicx"));
        NestedQueryBuilder bookPublisherCodeNestedFilter = QueryBuilders.nestedQuery("bookCategories.books", QueryBuilders.termQuery("bookCategories.books.bookPublisher.publisherCode", "PUB_03"));
        NestedQueryBuilder bookProviderNameNestedFilter = QueryBuilders.nestedQuery("bookCategories.books.bookPublisher.bookProvider", QueryBuilders.termQuery("bookCategories.books.bookPublisher.bookProvider.providerName", "PVD_01"));

        QueryBuilder topFilter = QueryBuilders.boolQuery().must(categoryNameTerm).must(bookAuthorNestedFilter).must(bookPublisherCodeNestedFilter).must(bookProviderNameNestedFilter);

        searchReq.setQuery(QueryBuilders.boolQuery().filter(topFilter));
    }

    @Test
    @Ignore
    public void testCreateAggDsl() {
        SearchRequestBuilder searchReq = new SearchRequestBuilder(new ElasticMockClient(), SearchAction.INSTANCE);
        searchReq.setSize(0);

        //NestedQueryBuilder categoryNameTerm = QueryBuilders.nestedQuery("bookCategories", QueryBuilders.termQuery("bookCategories.categoryName", "ART"));
        TermsBuilder categoryNameAgg = AggregationBuilders.terms("agg_bookCategories.categoryName").field("bookCategories.categoryName");
        AbstractAggregationBuilder topAgg = AggregationBuilders.nested("nested_bookCategories.categoryName").path("bookCategories").subAggregation(categoryNameAgg);

        AbstractAggregationBuilder rtnAgg = AggregationBuilders.reverseNested("rtn_root").subAggregation(AggregationBuilders.terms("agg_name").field("name"));
        categoryNameAgg.subAggregation(rtnAgg);

        //NestedQueryBuilder bookAuthorNestedFilter = QueryBuilders.nestedQuery("bookCategories.books", QueryBuilders.termsFilter("bookCategories.books.bookAuthor", "bibicx"));
        //TermsBuilder bookAuthorAgg = AggregationBuilders.terms("agg_bookCategories.books.bookAuthor").field("bookCategories.books.bookAuthor");
        //AbstractAggregationBuilder secNestedAgg = AggregationBuilders.nested("nested_bookCategories.books.bookAuthor").path("bookCategories.books").subAggregation(bookAuthorAgg);

        //categoryNameAgg.subAggregation(secNestedAgg);

        //NestedQueryBuilder bookPublisherCodeNestedFilter = QueryBuilders.nestedQuery("bookCategories.books", QueryBuilders.termsFilter("bookCategories.books.bookPublisher.publisherCode", "PUB_03"));
        //NestedQueryBuilder bookProviderNameNestedFilter = QueryBuilders.nestedQuery("bookCategories.books.bookPublisher.bookProvider", QueryBuilders.termsFilter("bookCategories.books.bookPublisher.bookProvider.providerName", "PVD_01"));

        //QueryBuilder topFilter = QueryBuilders.boolQuery().must(categoryNameTerm).must(bookAuthorNestedFilter).must(bookPublisherCodeNestedFilter).must(bookProviderNameNestedFilter);

        searchReq.addAggregation(topAgg);

    }

    @Test
    public void testNotExpr() {
        String sql = "select *  from index.order where not(c = 0)";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        QueryBuilder f1 = QueryBuilders.boolQuery().must(
                QueryBuilders.boolQuery().mustNot(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.termQuery("c", 0)
                        )
                )
        );
        QueryBuilder f2 = parseResult.getWhereCondition();
        Assert.assertEquals(f1.toString(), f2.toString());


        sql = "select *  from index.order where a > 0 and not(c = 0)";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);

        f1 = QueryBuilders.boolQuery().must(
                QueryBuilders.rangeQuery("a").gt(0)).must(

                QueryBuilders.boolQuery().mustNot(
                        QueryBuilders.boolQuery().must(
                                QueryBuilders.termQuery("c", 0)
                        )
                )
        );
        f2 = parseResult.getWhereCondition();
        Assert.assertEquals(f1.toString(), f2.toString());


        sql = "select *  from index.order where a > 0 and not(c = 0 or b = 1)";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);

        f1 = QueryBuilders.boolQuery().must(
                QueryBuilders.rangeQuery("a").gt(0)).must(

                QueryBuilders.boolQuery().mustNot(
                        QueryBuilders.boolQuery().should(
                                QueryBuilders.termQuery("c", 0)).should(
                                QueryBuilders.termQuery("b", 1)
                        )
                )
        );
        f2 = parseResult.getWhereCondition();
        Assert.assertEquals(f1.toString(), f2.toString());


        sql = "select *  from index.order where a > 0 and not(c = 0 or not(b = 1))";
        sql2DslParser = new ElasticSql2DslParser();
        parseResult = sql2DslParser.parse(sql);

        f1 = QueryBuilders.boolQuery().must(
                QueryBuilders.rangeQuery("a").gt(0)).must(

                QueryBuilders.boolQuery().mustNot(
                        QueryBuilders.boolQuery().should(
                                QueryBuilders.termQuery("c", 0)).should(
                                QueryBuilders.boolQuery().mustNot(
                                        QueryBuilders.boolQuery().must(
                                                QueryBuilders.termQuery("b", 1)
                                        )
                                )
                        )
                )
        );
        f2 = parseResult.getWhereCondition();
        Assert.assertEquals(f1.toString(), f2.toString());
    }
}
