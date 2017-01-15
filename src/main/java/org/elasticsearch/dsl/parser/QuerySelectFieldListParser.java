package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.google.common.collect.Lists;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.ElasticSqlIdentifier;
import org.elasticsearch.dsl.ParseActionListener;
import org.elasticsearch.dsl.parser.helper.ElasticSqlIdentifierHelper;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

import java.util.List;

public class QuerySelectFieldListParser implements QueryParser {

    public static final String SQL_FIELD_MATCH_ALL = "*";

    private ParseActionListener parseActionListener;

    public QuerySelectFieldListParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();

        final List<String> selectFields = Lists.newLinkedList();
        for (SQLSelectItem selectField : queryBlock.getSelectList()) {
            ElasticSqlIdentifier sqlIdentifier = ElasticSqlIdentifierHelper.parseSqlIdentifier(selectField.getExpr(), dslContext.getParseResult().getQueryAs(), new ElasticSqlIdentifierHelper.ElasticSqlSinglePropertyFunc() {
                @Override
                public void parse(String propertyName) {
                    if (!SQL_FIELD_MATCH_ALL.equals(propertyName)) {
                        selectFields.add(propertyName);
                    }
                }
            }, new ElasticSqlIdentifierHelper.ElasticSqlPathPropertyFunc() {
                @Override
                public void parse(String propertyPath, String propertyName) {
                    selectFields.add(String.format("%s.%s", propertyPath, propertyName));
                }
            });
            onSelectFieldParse(sqlIdentifier);
        }
        dslContext.getParseResult().setQueryFieldList(selectFields);
    }

    private void onSelectFieldParse(ElasticSqlIdentifier field) {
        try {
            parseActionListener.onSelectFieldParse(field);
        } catch (Exception ex) {
            try {
                parseActionListener.onFailure(ex);
            } catch (Exception exp) {
                //ignore;
            }
        }
    }
}
