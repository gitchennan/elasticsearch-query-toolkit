package org.elasticsearch.dsl.parser.syntax.query.fulltext;

import com.alibaba.druid.sql.ast.SQLExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.enums.QueryFieldType;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.parser.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.syntax.sql.QueryFieldParser;
import org.elasticsearch.index.query.QueryBuilder;

public abstract class AbstractAtomFullTextQueryParser {

    protected ParseActionListener parseActionListener;

    public AbstractAtomFullTextQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    protected AtomQuery parseCondition(SQLExpr queryFieldExpr, Object[] parameters, String queryAs, IConditionFullTextQueryBuilder queryBuilder) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(queryFieldExpr, queryAs);

        AtomQuery atomQuery = null;
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            QueryBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), parameters);
            atomQuery = new AtomQuery(originalQuery);
        }

        if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            QueryBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), parameters);
            atomQuery = new AtomQuery(originalQuery, queryField.getNestedDocContextPath());
        }

        if (atomQuery == null) {
            throw new ElasticSql2DslException(String.format("[syntax error] query condition field can not support type[%s]", queryField.getQueryFieldType()));
        }

        onMatchAtomQueryConditionParse(queryField, parameters);

        return atomQuery;
    }

    private void onMatchAtomQueryConditionParse(ElasticSqlQueryField paramName, Object[] parameters) {
        try {
            parseActionListener.onMatchAtomQueryConditionParse(paramName, parameters);
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
