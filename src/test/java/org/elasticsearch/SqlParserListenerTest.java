package org.elasticsearch;

import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.enums.SQLConditionOperator;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.elasticsearch.dsl.parser.listener.ParseActionListenerAdapter;
import org.junit.Assert;
import org.junit.Test;

public class SqlParserListenerTest {
    @Test
    public void testParseActionListener() {
        String sql = "select id,status from index.order t where t.status = 'SUCCESS' and lastUpdatedTime > '2017-01-01' limit 5,15";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        sql2DslParser.parse(sql, new ParseActionListenerAdapter() {
            @Override
            public void onAtomConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator) {
                if (SQLConditionOperator.Equality == operator) {
                    Assert.assertEquals("status", paramName.getQueryFieldFullName());
                }

                if (SQLConditionOperator.GreaterThan == operator) {
                    Assert.assertEquals("lastUpdatedTime", paramName.getQueryFieldFullName());
                }
            }
        });
    }
}
