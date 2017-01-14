package org.elasticsearch.dsl.parser.helper;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.dsl.SQLIdentifierType;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;

import java.util.List;

public class ElasticSqlIdentifierHelper {

    /**
     * 字段标识符处理
     *
     * @param propertyNameExpr   待处理字段表达式
     * @param queryAsAlias       文档类型别名
     * @param singlePropertyFunc 非内嵌类型处理策略(inner/property)
     * @param pathPropertyFunc   内嵌类型处理逻辑(nested)
     * @return 标识符类型
     */
    public static SQLIdentifierType parseSqlIdentifier(final SQLExpr propertyNameExpr, final String queryAsAlias,
                                                       final ElasticSqlSinglePropertyFunc singlePropertyFunc,
                                                       final ElasticSqlPathPropertyFunc pathPropertyFunc) {
        if (propertyNameExpr instanceof SQLMethodInvokeExpr) {
            //如果指定是inner doc类型
            SQLMethodInvokeExpr innerObjExpr = (SQLMethodInvokeExpr) propertyNameExpr;
            if (ElasticSqlMethodInvokeHelper.INNER_DOC_METHOD.equalsIgnoreCase(innerObjExpr.getMethodName())) {
                ElasticSqlMethodInvokeHelper.checkInnerDocMethod(innerObjExpr);

                ElasticSqlIdentifierHelper.parseSqlIdf(innerObjExpr.getParameters().get(0), queryAsAlias, new ElasticSqlSinglePropertyFunc() {
                    @Override
                    public void parse(String propertyName) {
                        throw new ElasticSql2DslException("[syntax error] The arg of method inner_doc must contain property reference path");
                    }
                }, new ElasticSqlPathPropertyFunc() {
                    @Override
                    public void parse(String propertyPath, String propertyName) {
                        singlePropertyFunc.parse(String.format("%s.%s", propertyPath, propertyName));
                    }
                });
                return SQLIdentifierType.InnerDocProperty;
            }

            //如果执行是nested doc类型
            if (ElasticSqlMethodInvokeHelper.NESTED_DOC_METHOD.equalsIgnoreCase(innerObjExpr.getMethodName())) {
                ElasticSqlMethodInvokeHelper.checkNestedDocMethod(innerObjExpr);

                ElasticSqlIdentifierHelper.parseSqlIdf(innerObjExpr.getParameters().get(0), queryAsAlias, new ElasticSqlSinglePropertyFunc() {
                    @Override
                    public void parse(String propertyName) {
                        throw new ElasticSql2DslException("[syntax error] The arg of method nested_doc must contain property reference path");
                    }
                }, new ElasticSqlPathPropertyFunc() {
                    @Override
                    public void parse(String propertyPath, String propertyName) {
                        pathPropertyFunc.parse(propertyPath, propertyName);
                    }
                });
                return SQLIdentifierType.NestedDocProperty;
            }

            throw new ElasticSql2DslException("[syntax error] Sql identifier method only support nested_doc and inner_doc");
        }

        //默认按照inner doc或者property name来处理
        final List<Boolean> isInnerDocProperty = Lists.newLinkedList();
        ElasticSqlIdentifierHelper.parseSqlIdf(propertyNameExpr, queryAsAlias, new ElasticSqlSinglePropertyFunc() {
            @Override
            public void parse(String propertyName) {
                isInnerDocProperty.add(Boolean.FALSE);
                singlePropertyFunc.parse(propertyName);
            }
        }, new ElasticSqlPathPropertyFunc() {
            @Override
            public void parse(String propertyPath, String propertyName) {
                isInnerDocProperty.add(Boolean.TRUE);
                singlePropertyFunc.parse(String.format("%s.%s", propertyPath, propertyName));
            }
        });
        if (CollectionUtils.isNotEmpty(isInnerDocProperty)) {
            return isInnerDocProperty.get(0) ? SQLIdentifierType.InnerDocProperty : SQLIdentifierType.Property;
        }
        return SQLIdentifierType.MatchAllField;
    }

    private static void parseSqlIdf(final SQLExpr propertyNameExpr, final String queryAsAlias,
                                    final ElasticSqlSinglePropertyFunc singlePropertyFunc, final ElasticSqlPathPropertyFunc pathPropertyFunc) {
        //查询所有字段
        if (propertyNameExpr instanceof SQLAllColumnExpr) {
            return;
        }

        if (propertyNameExpr instanceof SQLIdentifierExpr) {
            singlePropertyFunc.parse(((SQLIdentifierExpr) propertyNameExpr).getName());
        } else if (propertyNameExpr instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) propertyNameExpr;
            StringBuffer ownerIdfNameBuilder = new StringBuffer();
            propertyExpr.getOwner().output(ownerIdfNameBuilder);

            if (StringUtils.isNotBlank(queryAsAlias)) {
                String ownerIdf = ownerIdfNameBuilder.toString();

                if (StringUtils.startsWithIgnoreCase(ownerIdf, queryAsAlias)) {
                    if (ownerIdf.length() > queryAsAlias.length()) {
                        //别名+path+属性名
                        pathPropertyFunc.parse(ownerIdf.substring(queryAsAlias.length() + 1, ownerIdf.length()), propertyExpr.getName());
                    } else if (queryAsAlias.equalsIgnoreCase(ownerIdf)) {
                        //别名+属性名
                        singlePropertyFunc.parse(propertyExpr.getName());
                    }
                } else {
                    //使用别名,但不以别名开头
                    pathPropertyFunc.parse(ownerIdfNameBuilder.toString(), propertyExpr.getName());
                }
            } else {
                //如果使用未使用别名
                pathPropertyFunc.parse(ownerIdfNameBuilder.toString(), propertyExpr.getName());
            }
        } else {
            throw new ElasticSql2DslException("[syntax error] Sql unSupport Identifier type: " + propertyNameExpr.getClass());
        }
    }


    @FunctionalInterface
    public interface ElasticSqlSinglePropertyFunc {
        void parse(String propertyName);
    }

    @FunctionalInterface
    public interface ElasticSqlPathPropertyFunc {
        void parse(String propertyPath, String propertyName);
    }

}
