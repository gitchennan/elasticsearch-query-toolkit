package org.es.test.query;

import org.es.sql.bean.ElasticSqlParseResult;
import org.es.sql.bean.ElasticSqlQueryField;
import org.es.sql.enums.SQLConditionOperator;
import org.es.sql.listener.ParseActionListenerAdapter;
import org.es.sql.parser.ElasticSql2DslParser;
import org.junit.Assert;
import org.junit.Test;

public class SqlParserListenerTest {
    @Test
    public void testParseActionListener() {
        String sql = "select id,status from index.order t where t.status = 'SUCCESS' and lastUpdatedTime > '2017-01-01' limit 5,15";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new ParseActionListenerAdapter() {
            @Override
            public void onAtomExactQueryConditionParse(ElasticSqlQueryField paramName, Object[] params, SQLConditionOperator operator) {
                if (SQLConditionOperator.Equality == operator) {
                    Assert.assertEquals("status", paramName.getQueryFieldFullName());
                }

                if (SQLConditionOperator.GreaterThan == operator) {
                    Assert.assertEquals("lastUpdatedTime", paramName.getQueryFieldFullName());
                }
            }
        });
        System.out.println(parseResult.toDsl());
    }
}
