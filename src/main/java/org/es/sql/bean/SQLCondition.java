package org.es.sql.bean;

import com.google.common.collect.Lists;
import org.es.sql.enums.SQLBoolOperator;
import org.es.sql.enums.SQLConditionType;

import java.util.List;

public class SQLCondition {
    //条件类型
    private SQLConditionType conditionType;
    //运算符
    private SQLBoolOperator operator;
    //条件集合
    private List<AtomQuery> queryList;

    public SQLCondition(AtomQuery atomQuery) {
        this(atomQuery, SQLConditionType.Atom);
    }

    public SQLCondition(AtomQuery atomQuery, SQLConditionType SQLConditionType) {
        this.queryList = Lists.newArrayList(atomQuery);
        this.conditionType = SQLConditionType;
    }

    public SQLCondition(List<AtomQuery> queryList, SQLBoolOperator operator) {
        this.queryList = queryList;
        this.operator = operator;
        this.conditionType = SQLConditionType.Combine;
    }


    public SQLConditionType getSQLConditionType() {
        return conditionType;
    }

    public SQLBoolOperator getOperator() {
        return operator;
    }

    public List<AtomQuery> getQueryList() {
        return queryList;
    }
}
