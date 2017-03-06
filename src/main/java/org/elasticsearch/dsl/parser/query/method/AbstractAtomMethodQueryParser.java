package org.elasticsearch.dsl.parser.query.method;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.Maps;
import org.elasticsearch.dsl.bean.AtomQuery;
import org.elasticsearch.dsl.bean.ElasticSqlQueryField;
import org.elasticsearch.dsl.enums.QueryFieldType;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.dsl.listener.ParseActionListener;
import org.elasticsearch.dsl.parser.sql.QueryFieldParser;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

public abstract class AbstractAtomMethodQueryParser {

    protected static final String COMMA = ",";

    protected static final String COLON = ":";

    protected ParseActionListener parseActionListener;

    public AbstractAtomMethodQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    protected abstract void checkQueryMethod(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs);

    protected abstract AtomQuery parseMethodQueryExpr(SQLMethodInvokeExpr matchQueryExpr, String queryAs, Object[] sqlArgs);


    public final AtomQuery parseAtomMethodQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, Object[] sqlArgs) {
        checkQueryMethod(methodQueryExpr, queryAs, sqlArgs);

        return parseMethodQueryExpr(methodQueryExpr, queryAs, sqlArgs);
    }

    protected AtomQuery parseCondition(SQLExpr queryFieldExpr, Object[] parameters, String queryAs, IConditionMethodQueryBuilder queryBuilder) {
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

        onAtomMethodQueryConditionParse(queryField, parameters);

        return atomQuery;
    }

    private void onAtomMethodQueryConditionParse(ElasticSqlQueryField paramName, Object[] parameters) {
        try {
            parseActionListener.onAtomMethodQueryConditionParse(paramName, parameters);
        }
        catch (Exception ex) {
            try {
                parseActionListener.onFailure(ex);
            } catch (Exception exp) {
                //ignore;
            }
        }
    }

    protected Map<String, String> buildExtraMethodQueryParamsMap(String strMatchQueryParams) {
        try {
            Map<String, String> extraParamMap = Maps.newHashMap();
            for (String paramPair : strMatchQueryParams.split(COMMA)) {
                String[] paramPairArr = paramPair.split(COLON);
                extraParamMap.put(paramPairArr[0].trim(), paramPairArr[1].trim());
            }
            return extraParamMap;
        }
        catch (Exception ex) {
            throw new ElasticSql2DslException("[fulltext_query] Fulltext match query method param format error!");
        }
    }
}
