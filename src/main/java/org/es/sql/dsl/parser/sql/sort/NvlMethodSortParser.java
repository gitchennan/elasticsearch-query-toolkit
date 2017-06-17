package org.es.sql.dsl.parser.sql.sort;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.ImmutableList;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.es.sql.dsl.bean.ElasticSqlQueryField;
import org.es.sql.dsl.enums.SortOption;
import org.es.sql.dsl.exception.ElasticSql2DslException;
import org.es.sql.dsl.parser.query.method.MethodInvocation;
import org.es.sql.dsl.parser.sql.QueryFieldParser;

import java.util.List;
import java.util.Map;

public class NvlMethodSortParser extends AbstractMethodSortParser {

    public static final List<String> NVL_METHOD = ImmutableList.of("nvl", "is_null", "isnull");

    @Override
    public List<String> defineMethodNames() {
        return NVL_METHOD;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation nvlMethodInvocation) throws ElasticSql2DslException {
        if (!isMatchMethodInvocation(nvlMethodInvocation)) {
            throw new ElasticSql2DslException("[syntax error] Sql sort condition only support nvl method invoke");
        }

        int methodParameterCount = nvlMethodInvocation.getParameterCount();
        if (methodParameterCount == 0 || methodParameterCount > 3) {
            throw new ElasticSql2DslException(String.format("[syntax error] There is no %s args method named nvl", methodParameterCount));
        }

        SQLExpr fieldArg = nvlMethodInvocation.getParameter(0);
        SQLExpr valueArg = nvlMethodInvocation.getParameter(1);

        if (!(fieldArg instanceof SQLPropertyExpr) && !(fieldArg instanceof SQLIdentifierExpr)) {
            throw new ElasticSql2DslException("[syntax error] The first arg of nvl method should be field param name");
        }

        if (!(valueArg instanceof SQLCharExpr) && !(valueArg instanceof SQLIntegerExpr) && !(valueArg instanceof SQLNumberExpr)) {
            throw new ElasticSql2DslException("[syntax error] The second arg of nvl method should be number or string");
        }

        if (methodParameterCount == 3) {
            SQLExpr sortModArg = nvlMethodInvocation.getParameter(2);
            if (!(sortModArg instanceof SQLCharExpr)) {
                throw new ElasticSql2DslException("[syntax error] The third arg of nvl method should be string");
            }
            String sortModeText = ((SQLCharExpr) sortModArg).getText();
            if (!SortOption.AVG.mode().equalsIgnoreCase(sortModeText) && !SortOption.MIN.mode().equalsIgnoreCase(sortModeText)
                    && !SortOption.MAX.mode().equalsIgnoreCase(sortModeText) && !SortOption.SUM.mode().equalsIgnoreCase(sortModeText)) {
                throw new ElasticSql2DslException("[syntax error] The third arg of nvl method should be one of the string[min,max,avg,sum]");
            }
        }
    }

    @Override
    protected SortBuilder parseMethodSortBuilderWithExtraParams(
            MethodInvocation sortMethodInvocation, SortOrder order, Map<String, Object> extraParamMap) throws ElasticSql2DslException {

        String queryAs = sortMethodInvocation.getQueryAs();
        SQLExpr fieldExpr = sortMethodInvocation.getParameter(0);
        Object valueArg = sortMethodInvocation.getParameterAsObject(1);

        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField sortField = queryFieldParser.parseConditionQueryField(fieldExpr, queryAs);

        return ParseSortBuilderHelper.parseBasedOnFieldSortBuilder(sortField, new ConditionSortBuilder() {
            @Override
            public FieldSortBuilder buildSort(String idfName) {
                FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort(idfName).order(order).missing(valueArg);

                if (sortMethodInvocation.getParameterCount() == 3) {
                    String sortModeText = sortMethodInvocation.getParameterAsString(2);
                    fieldSortBuilder.sortMode(SortOption.get(sortModeText).mode());
                }
                return fieldSortBuilder;
            }
        });
    }
}
