package org.es.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.SQLExpr;
import org.elasticsearch.index.query.QueryBuilder;
import org.es.sql.bean.AtomQuery;
import org.es.sql.bean.ElasticSqlQueryField;
import org.es.sql.enums.QueryFieldType;
import org.es.sql.enums.SQLConditionOperator;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.listener.ParseActionListener;
import org.es.sql.parser.sql.QueryFieldParser;

public abstract class AbstractAtomExactQueryParser {

    protected ParseActionListener parseActionListener;

    public AbstractAtomExactQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    protected AtomQuery parseCondition(SQLExpr queryFieldExpr, SQLConditionOperator operator, Object[] params, String queryAs, IConditionExactQueryBuilder queryBuilder) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(queryFieldExpr, queryAs);

        AtomQuery atomQuery = null;
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            QueryBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), operator, params);
            atomQuery = new AtomQuery(originalQuery);
        }

        if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            QueryBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), operator, params);
            atomQuery = new AtomQuery(originalQuery, queryField.getNestedDocContextPath());
        }

        if (atomQuery == null) {
            throw new ElasticSql2DslException(String.format("[syntax error] where condition field can not support type[%s]", queryField.getQueryFieldType()));
        }

        parseActionListener.onAtomExactQueryConditionParse(queryField, params, operator);

        return atomQuery;
    }
}
