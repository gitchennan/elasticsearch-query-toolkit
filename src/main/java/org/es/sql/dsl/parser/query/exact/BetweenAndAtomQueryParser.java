package org.es.sql.dsl.parser.query.exact;

import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.es.sql.dsl.bean.AtomFilter;
import org.es.sql.dsl.enums.SQLConditionOperator;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.helper.ElasticSqlArgTransferHelper;
import org.es.sql.dsl.listener.ParseActionListener;

public class BetweenAndAtomQueryParser extends AbstractAtomExactQueryParser {

    public BetweenAndAtomQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public AtomFilter parseBetweenAndQuery(SQLBetweenExpr betweenAndExpr, String queryAs, Object[] sqlArgs) {
        Object from = ElasticSqlArgTransferHelper.transferSqlArg(betweenAndExpr.getBeginExpr(), sqlArgs);
        Object to = ElasticSqlArgTransferHelper.transferSqlArg(betweenAndExpr.getEndExpr(), sqlArgs);

        if (from == null || to == null) {
            throw new ElasticSql2DslException("[syntax error] Between Expr only support one of [number,date] arg type");
        }

        return parseCondition(betweenAndExpr.getTestExpr(), SQLConditionOperator.BetweenAnd, new Object[]{from, to}, queryAs, new IConditionExactQueryBuilder() {
            @Override
            public FilterBuilder buildQuery(String queryFieldName, SQLConditionOperator operator, Object[] rightParamValues) {
                return FilterBuilders.rangeFilter(queryFieldName).gte(rightParamValues[0]).lte(rightParamValues[1]);
            }
        });
    }
}
