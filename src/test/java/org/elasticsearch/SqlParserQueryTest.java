package org.elasticsearch;

import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.syntax.ElasticSql2DslParser;
import org.junit.Test;


public class SqlParserQueryTest {
    @Test
    public void testParseQueryExpr() {

        String sql = "select id,status from index.order t "
                + "query match(t.productName, '皮鞋', 'minimum_should_match:75%') and match(t.productCode, '2700', 'operator:and')"
                + "where t.price > 1000 limit 5,15";

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());
    }
}
