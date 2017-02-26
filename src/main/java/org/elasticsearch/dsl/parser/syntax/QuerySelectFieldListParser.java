package org.elasticsearch.dsl.parser.syntax;

import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.google.common.collect.Lists;
import org.elasticsearch.dsl.bean.ElasticDslContext;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.parser.QueryParser;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
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
            ElasticSqlQueryField sqlIdentifier = ElasticSqlIdentifierHelper.parseSqlIdentifier(selectField.getExpr(), dslContext.getParseResult().getQueryAs(), new ElasticSqlIdentifierHelper.SQLFlatFieldFunc() {
                @Override
                public void parse(String flatFieldName) {
                    if (!SQL_FIELD_MATCH_ALL.equals(flatFieldName)) {
                        selectFields.add(flatFieldName);
                    }
                }
            }, new ElasticSqlIdentifierHelper.SQLNestedFieldFunc() {
                @Override
                public void parse(String nestedDocPath, String fieldName) {
                    selectFields.add(String.format("%s.%s", nestedDocPath, fieldName));
                }
            });
            onSelectFieldParse(sqlIdentifier);
        }
        dslContext.getParseResult().setQueryFieldList(selectFields);
    }

    private void onSelectFieldParse(ElasticSqlQueryField field) {
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
