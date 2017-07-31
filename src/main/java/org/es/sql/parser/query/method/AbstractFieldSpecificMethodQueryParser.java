package org.es.sql.parser.query.method;

import org.elasticsearch.index.query.QueryBuilder;
import org.es.sql.bean.AtomQuery;
import org.es.sql.bean.ElasticSqlQueryField;
import org.es.sql.enums.QueryFieldType;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.listener.ParseActionListener;
import org.es.sql.parser.query.method.expr.FieldSpecificMethodExpression;
import org.es.sql.parser.sql.QueryFieldParser;

import java.util.Map;

public abstract class AbstractFieldSpecificMethodQueryParser extends ParameterizedMethodQueryParser implements FieldSpecificMethodExpression {

    protected ParseActionListener parseActionListener;

    public AbstractFieldSpecificMethodQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    protected abstract QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams);

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        //ignore extra params, subclass can override if necessary
        return null;
    }

    @Override
    protected AtomQuery parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(defineFieldExpr(invocation), invocation.getQueryAs());

        AtomQuery atomQuery = null;
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            QueryBuilder originalQuery = buildQuery(invocation, queryField.getQueryFieldFullName(), extraParamMap);
            atomQuery = new AtomQuery(originalQuery);
        }

        if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            QueryBuilder originalQuery = buildQuery(invocation, queryField.getQueryFieldFullName(), extraParamMap);
            atomQuery = new AtomQuery(originalQuery, queryField.getNestedDocContextPath());
        }

        if (atomQuery == null) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] query field can not support type[%s]", queryField.getQueryFieldType()));
        }

        parseActionListener.onAtomMethodQueryConditionParse(queryField, invocation.getSqlArgs().getArgs());

        return atomQuery;
    }
}
