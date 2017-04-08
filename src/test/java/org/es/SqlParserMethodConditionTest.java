package org.es;

import org.es.sql.dsl.bean.ElasticSqlParseResult;
import org.es.sql.dsl.parser.ElasticSql2DslParser;
import org.junit.Test;


public class SqlParserMethodConditionTest {
    @Test
    public void testScriptExpr() {

        String script = "if(doc[\"advicePrice\"].empty) return false; if(my_var * doc[\"minPrice\"].value/doc[\"advicePrice\"].value > 0.363) return true; else return false;";
        String sql = String.format("select * from index.product where script_query('%s', 'my_var:2.1f')", script);

        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

        System.out.println(parseResult.toDsl());

    }
}
