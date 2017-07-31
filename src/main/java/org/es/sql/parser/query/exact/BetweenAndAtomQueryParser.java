package org.es.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.es.sql.bean.AtomQuery;
import org.es.sql.bean.SQLArgs;
import org.es.sql.enums.SQLConditionOperator;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.helper.ElasticSqlArgConverter;
import org.es.sql.listener.ParseActionListener;


public class BetweenAndAtomQueryParser extends AbstractAtomExactQueryParser {

    public BetweenAndAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public AtomQuery parseBetweenAndQuery(SQLBetweenExpr betweenAndExpr, String queryAs, SQLArgs SQLArgs) {
        Object from = ElasticSqlArgConverter.convertSqlArg(betweenAndExpr.getBeginExpr(), SQLArgs);
        Object to = ElasticSqlArgConverter.convertSqlArg(betweenAndExpr.getEndExpr(), SQLArgs);

        if (from == null || to == null) {
            throw new ElasticSql2DslException("[syntax error] Between Expr only support one of [number,date] arg type");
        }

        return parseCondition(betweenAndExpr.getTestExpr(), SQLConditionOperator.BetweenAnd, new Object[]{from, to}, queryAs, new IConditionExactQueryBuilder() {
            @Override
            public QueryBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues) {
                return QueryBuilders.rangeQuery(queryFieldName).gte(rightParamValues[0]).lte(rightParamValues[1]);
            }
        });
    }
}
