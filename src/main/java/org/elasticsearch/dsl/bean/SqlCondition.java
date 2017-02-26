package org.elasticsearch.dsl.bean;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.google.common.collect.Lists;
import org.elasticsearch.index.query.FilterBuilder;

import java.util.List;

public class SqlCondition {
    //是否AND/OR运算
    private boolean isAndOr = false;
    //运算符
    private SQLBinaryOperator operator;
    //条件集合
    private List<FilterBuilder> filterList;

    public SqlCondition(FilterBuilder atomFilter) {
        filterList = Lists.newArrayList(atomFilter);
        isAndOr = false;
    }

    public SqlCondition(List<FilterBuilder> filterList, SQLBinaryOperator operator) {
        this.filterList = filterList;
        isAndOr = true;
        this.operator = operator;
    }

    public boolean isAndOr() {
        return isAndOr;
    }

    public SQLBinaryOperator getOperator() {
        return operator;
    }

    public List<FilterBuilder> getFilterList() {
        return filterList;
    }
}
