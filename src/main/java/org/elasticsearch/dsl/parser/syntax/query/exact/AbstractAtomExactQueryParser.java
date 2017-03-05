package org.elasticsearch.dsl.parser.syntax.query.exact;

import com.alibaba.druid.sql.ast.SQLExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.enums.QueryFieldType;
import org.elasticsearch.dsl.enums.SQLConditionOperator;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.syntax.sql.QueryFieldParser;
import org.elasticsearch.index.query.QueryBuilder;

public abstract class AbstractAtomExactQueryParser {

    protected ParseActionListener parseActionListener;

    public AbstractAtomExactQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    protected AtomQuery parseCondition(SQLExpr queryFieldExpr, SQLConditionOperator operator, Object[] rightParamValues, String queryAs, IConditionExactQueryBuilder queryBuilder) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(queryFieldExpr, queryAs);

        AtomQuery atomQuery = null;
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            QueryBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), operator, rightParamValues);
            atomQuery = new AtomQuery(originalQuery);
        }

        if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            QueryBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), operator, rightParamValues);
            atomQuery = new AtomQuery(originalQuery, queryField.getNestedDocContextPath());
        }

        if (atomQuery == null) {
            throw new ElasticSql2DslException(String.format("[syntax error] where condition field can not support type[%s]", queryField.getQueryFieldType()));
        }

        onExactAtomQueryConditionParse(queryField, rightParamValues, operator);

        return atomQuery;
    }

    private void onExactAtomQueryConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator) {
        try {
            parseActionListener.onExactAtomQueryConditionParse(paramName, paramValues, operator);
        }
        catch (Exception ex) {
            try {
                parseActionListener.onFailure(ex);
            }
            catch (Exception exp) {
                //ignore;
            }
        }
    }

}
