package org.elasticsearch.dsl.parser;

import com.google.common.collect.Lists;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;

public class QuerySelectFieldListParser implements ElasticSqlParser {

    public static final String SQL_FIELD_MATCH_ALL = "*";

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();

        List<String> selectFields = Lists.newLinkedList();
        queryBlock.getSelectList().stream().forEach(
                selectField -> ElasticSqlIdfParser.parseSqlIdentifier(selectField.getExpr(), dslContext.getQueryAs(),
                        idfName -> {
                            if (!SQL_FIELD_MATCH_ALL.equals(idfName)) {
                                selectFields.add(idfName);
                            }
                        },
                        (nestPath, idfName) -> {
                            selectFields.add(String.format("%s.%s", nestPath, idfName));
                        }
                )
        );
        dslContext.setQueryFieldList(selectFields);
    }
}
