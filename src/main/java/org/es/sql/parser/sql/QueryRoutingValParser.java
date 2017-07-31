package org.es.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.es.sql.bean.ElasticDslContext;
import org.es.sql.druid.ElasticSqlSelectQueryBlock;
import org.es.sql.exception.ElasticSql2DslException;
import org.es.sql.helper.ElasticSqlArgConverter;
import org.es.sql.listener.ParseActionListener;

import java.util.List;

public class QueryRoutingValParser implements QueryParser {

    private ParseActionListener parseActionListener;

    public QueryRoutingValParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    @Override
    public void parse(ElasticDslContext dslContext) {
        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) dslContext.getQueryExpr().getSubQuery().getQuery();
        if (queryBlock.getRouting() != null && CollectionUtils.isNotEmpty(queryBlock.getRouting().getRoutingValues())) {
            List<String> routingStringValues = Lists.newLinkedList();
            for (SQLExpr routingVal : queryBlock.getRouting().getRoutingValues()) {
                if (routingVal instanceof SQLCharExpr) {
                    routingStringValues.add(((SQLCharExpr) routingVal).getText());
                }
                else if (routingVal instanceof SQLVariantRefExpr) {
                    Object targetVal = ElasticSqlArgConverter.convertSqlArg(routingVal, dslContext.getSQLArgs());
                    routingStringValues.add(targetVal.toString());
                }
                else {
                    throw new ElasticSql2DslException("[syntax error] Index routing val must be a string");
                }
            }
            dslContext.getParseResult().setRoutingBy(routingStringValues);

            parseActionListener.onRoutingValuesParse(routingStringValues);
        }
    }
}
