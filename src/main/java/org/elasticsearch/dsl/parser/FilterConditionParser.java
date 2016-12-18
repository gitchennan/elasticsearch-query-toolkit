package org.elasticsearch.dsl.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import org.elasticsearch.dsl.ElasticDslContext;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.elasticsearch.sql.ElasticSqlSelectQueryBlock;

public class FilterConditionParser {
    public static void parseFilterCondition(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        parseFilterCondition(dslContext.boolFilter(), queryBlock.getWhere());

    }

    private static void parseFilterCondition(BoolFilterBuilder filterBuilder, SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLBinaryOperator binaryOperator = sqlBinOpExpr.getOperator();

            if (isValidBinOperator(binaryOperator)) {

            }
        } else if (sqlExpr instanceof SQLInListExpr) {
            SQLInListExpr inListExpr = (SQLInListExpr) sqlExpr;

            if (inListExpr.getExpr() instanceof SQLIdentifierExpr) {
                TermsFilterBuilder inFilter = FilterBuilders.inFilter(
                        ((SQLIdentifierExpr) inListExpr.getExpr()).getName(),
                        inListExpr.getTargetList().toArray(new Object[inListExpr.getTargetList().size()])
                );
                if (inListExpr.isNot()) {
                    filterBuilder.mustNot(inFilter);
                } else {
                    filterBuilder.must(inFilter);
                }
            }

            if (inListExpr.getExpr() instanceof SQLPropertyExpr) {

            }
        } else {
            throw new ElasticSql2DslException("[syntax error] Can not support Op type: " + sqlExpr.getClass());
        }
    }

    private static boolean isValidBinOperator(SQLBinaryOperator binaryOperator) {
        return binaryOperator == SQLBinaryOperator.BooleanAnd
                || binaryOperator == SQLBinaryOperator.Equality
                || binaryOperator == SQLBinaryOperator.NotEqual
                || binaryOperator == SQLBinaryOperator.BooleanOr
                || binaryOperator == SQLBinaryOperator.GreaterThan
                || binaryOperator == SQLBinaryOperator.GreaterThanOrEqual
                || binaryOperator == SQLBinaryOperator.LessThan
                || binaryOperator == SQLBinaryOperator.LessThanOrEqual
                || binaryOperator == SQLBinaryOperator.LessThanOrGreater
                || binaryOperator == SQLBinaryOperator.Is
                || binaryOperator == SQLBinaryOperator.IsNot;
    }

}
