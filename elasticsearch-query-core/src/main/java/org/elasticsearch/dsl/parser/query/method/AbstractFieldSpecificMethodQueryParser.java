package org.elasticsearch.dsl.parser.query.method;

import com.alibaba.druid.sql.ast.SQLExpr;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.enums.QueryFieldType;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.sql.QueryFieldParser;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

public abstract class AbstractFieldSpecificMethodQueryParser extends ParameterizedMethodQueryParser {

    protected ParseActionListener parseActionListener;

    public AbstractFieldSpecificMethodQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    protected abstract QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams);

    protected abstract SQLExpr defineFieldExpr(MethodInvocation invocation);

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

        onAtomMethodQueryConditionParse(queryField, invocation.getSqlArgs());

        return atomQuery;
    }

    private void onAtomMethodQueryConditionParse(ElasticSqlQueryField paramName, Object[] parameters) {
        try {
            parseActionListener.onAtomMethodQueryConditionParse(paramName, parameters);
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
