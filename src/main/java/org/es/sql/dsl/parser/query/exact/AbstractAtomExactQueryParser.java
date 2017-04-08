package org.es.sql.dsl.parser.query.exact;


import com.alibaba.druid.sql.ast.SQLExpr;
import org.elasticsearch.index.query.FilterBuilder;
import org.es.sql.dsl.bean.AtomFilter;
import org.es.sql.dsl.bean.ElasticSqlQueryField;
import org.es.sql.dsl.enums.QueryFieldType;
import org.es.sql.dsl.enums.SQLConditionOperator;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.listener.ParseActionListener;
import org.es.sql.dsl.parser.sql.QueryFieldParser;

public abstract class AbstractAtomExactQueryParser {

    protected ParseActionListener parseActionListener;

    public AbstractAtomExactQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    protected AtomFilter parseCondition(SQLExpr queryFieldExpr, SQLConditionOperator operator, Object[] rightParamValues, String queryAs, IConditionExactQueryBuilder queryBuilder) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(queryFieldExpr, queryAs);

        AtomFilter atomQuery = null;
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            FilterBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), operator, rightParamValues);
            atomQuery = new AtomFilter(originalQuery);
        }

        if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            FilterBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), operator, rightParamValues);
            atomQuery = new AtomFilter(originalQuery, queryField.getNestedDocContextPath());
        }

        if (atomQuery == null) {
            throw new ElasticSql2DslException(String.format("[syntax error] where condition field can not support type[%s]", queryField.getQueryFieldType()));
        }

        onExactAtomQueryConditionParse(queryField, rightParamValues, operator);

        return atomQuery;
    }

    private void onExactAtomQueryConditionParse(ElasticSqlQueryField paramName, Object[] paramValues, SQLConditionOperator operator) {
        try {
            parseActionListener.onAtomFilterConditionParse(paramName, paramValues, operator);
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
