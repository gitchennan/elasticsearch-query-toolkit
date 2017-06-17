package org.es.test.query;

import org.es.sql.bean.ElasticSqlParseResult;
import org.es.sql.parser.ElasticSql2DslParser;
import org.junit.Test;


public class SqlParserQueryTest {
    @Test
    public void testParseMatchQueryExpr() {

        String sql = "select id,status from index.order t "
                + "query match(t.productName, '皮鞋', 'minimum_should_match:75%,boost:2.0f') and match(t.productCode, '2700', 'operator:and')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseMultiMatchQueryExpr() {

        String sql = "select id,status from index.order t "
                + "query multiMatch('productName,productCode', '皮鞋', 'minimum_should_match:75%,boost:2.0f')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseQueryStringQueryExpr() {

        String sql = "select id,status from index.order t "
                + "query queryString('皮鞋', 'fields:productName,productCode', 'boost:2.0f')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseSimpleQueryStringQueryExpr() {
        String sql = "select id,status from index.order t "
                + "query simpleQueryString('皮鞋', 'fields:productName,productCode^2', 'flags:OR|AND|PREFIX')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParsePrefixQueryExpr() {
        String sql = "select id,status from index.order t "
                + "query prefix(t.productName, '皮鞋', 'boost:2.0f')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseTermQueryExpr() {
        String sql = "select id,status from index.order t "
                + "query term(t.productName, '皮鞋', 'boost:2.0f')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseTermsQueryExpr() {
        String sql = "select id,status from index.order t "
                + "query terms(t.productName, '皮鞋', '凉鞋', 'boost:2.0f')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseWildcardQueryExpr() {
        String sql = "select id,status from index.order t "
                + "query wildcard(t.productName, '*鞋', 'boost:2.0f')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseRegexpQueryExpr() {
        String sql = "select id,status from index.order t "
                + "query regexp(t.productName, '*鞋', 'boost:2.0f,flags:INTERSECTION|COMPLEMENT')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }

    @Test
    public void testParseFuzzyQueryExpr() {
        String sql = "select id,status from index.order t "
                + "query fuzzy(t.productName, '鞋', 'boost:2.0f,fuzziness:2')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }
}
