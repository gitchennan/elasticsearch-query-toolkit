package org.elasticsearch;

import com.google.common.collect.Lists;
import org.elasticsearch.dsl.enums.QueryFieldType;
import org.elasticsearch.dsl.parser.ElasticSql2DslParser;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.bean.ElasticSqlParseResult;
import org.elasticsearch.dsl.parser.listener.ParseActionListenerAdapter;
import org.elasticsearch.dsl.enums.SQLConditionOperator;
import org.junit.Test;

import java.util.List;

public class SqlParserListenerTest {
    @Test
    public void testParseActionListener() {
        final List<String> avaliableIndex = Lists.newArrayList("2016");

        String sql = "select id,productStatus from index.trx_order trx where trx.status = 'SUCCESS' and updatedAt > '2017-01-01' limit 5,15";
        ElasticSql2DslParser sql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql, new ParseActionListenerAdapter() {
            @Override
            public void onAtomConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator) {
                if (paramName.getQueryFieldType() == QueryFieldType.RootDocField
                        && "updatedAt".equals(paramName.getSimpleQueryFieldName())) {
                    if (SQLConditionOperator.GreaterThan == operator || SQLConditionOperator.GreaterThanOrEqual == operator) {

                    }

                    if (SQLConditionOperator.LessThan == operator || SQLConditionOperator.LessThanOrEqual == operator) {

                    }

                    if (SQLConditionOperator.BetweenAnd == operator) {

                    }

                    if (SQLConditionOperator.Equality == operator || SQLConditionOperator.In == operator) {

                    }
                }
            }
        });
        System.out.println(parseResult.toDsl());
    }
}
