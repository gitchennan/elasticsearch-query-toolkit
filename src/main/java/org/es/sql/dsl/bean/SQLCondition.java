package org.es.sql.dsl.bean;


import com.google.common.collect.Lists;
import org.es.sql.dsl.enums.SQLBoolOperator;
import org.es.sql.dsl.enums.SQLConditionType;

import java.util.List;

public class SQLCondition {
    //条件类型
    private SQLConditionType conditionType;
    //运算符
    private SQLBoolOperator operator;
    //条件集合
    private List<AtomFilter> filterList;

    public SQLCondition(AtomFilter atomFilter) {
        this.filterList = Lists.newArrayList(atomFilter);
        this.conditionType = SQLConditionType.Atom;
    }

    public SQLCondition(AtomFilter atomFilter, SQLConditionType SQLConditionType) {
        this.filterList = Lists.newArrayList(atomFilter);
        this.conditionType = SQLConditionType;
    }

    public SQLCondition(List<AtomFilter> filterList, SQLBoolOperator operator) {
        this.filterList = filterList;
        this.operator = operator;
        this.conditionType = SQLConditionType.Combine;
    }


    public SQLConditionType getSQLConditionType() {
        return conditionType;
    }

    public SQLBoolOperator getOperator() {
        return operator;
    }

    public List<AtomFilter> getFilterList() {
        return filterList;
    }
}
