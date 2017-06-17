package org.es.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.es.sql.bean.ElasticSqlQueryField;
import org.es.sql.bean.ElasticSqlQueryFields;
import org.es.sql.bean.QueryFieldReferenceNode;
import org.es.sql.bean.QueryFieldReferencePath;
import org.es.sql.exception.ElasticSql2DslException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class QueryFieldParser {
    private static final String NESTED_DOC_IDF = "$";
    private static final String FIELD_REF_PATH_DOT = ".";

    private static List<QueryFieldReferenceNode> parseQueryFieldExprToRefPath(SQLExpr queryFieldExpr) {
        List<QueryFieldReferenceNode> referencePathNodes = Lists.newLinkedList();

        if (queryFieldExpr instanceof SQLIdentifierExpr) {
            String idfName = ((SQLIdentifierExpr) queryFieldExpr).getName();
            QueryFieldReferenceNode referenceNode = buildReferenceNode(idfName);
            referencePathNodes.add(referenceNode);

            return referencePathNodes;
        }

        if (queryFieldExpr instanceof SQLPropertyExpr) {
            List<String> queryFieldTextList = Lists.newLinkedList();

            SQLExpr tmpLoopExpr = queryFieldExpr;

            while ((tmpLoopExpr != null) && (tmpLoopExpr instanceof SQLPropertyExpr)) {
                queryFieldTextList.add(((SQLPropertyExpr) tmpLoopExpr).getName());
                tmpLoopExpr = ((SQLPropertyExpr) tmpLoopExpr).getOwner();
            }

            if (tmpLoopExpr instanceof SQLIdentifierExpr) {
                queryFieldTextList.add(((SQLIdentifierExpr) tmpLoopExpr).getName());
            }

            Collections.reverse(queryFieldTextList);
            for (String strRefNode : queryFieldTextList) {
                QueryFieldReferenceNode referenceNode = buildReferenceNode(strRefNode);
                referencePathNodes.add(referenceNode);
            }

            return referencePathNodes;
        }

        throw new ElasticSql2DslException(String.format("[syntax error] can not support query field type[%s]", queryFieldExpr.toString()));
    }

    private static QueryFieldReferenceNode buildReferenceNode(String strRefNodeName) {
        QueryFieldReferenceNode referenceNode = null;
        if (strRefNodeName.startsWith(NESTED_DOC_IDF)) {
            if (NESTED_DOC_IDF.equals(strRefNodeName)) {
                throw new ElasticSql2DslException("[syntax error] nested doc query field can not be blank");
            }
            referenceNode = new QueryFieldReferenceNode(strRefNodeName.substring(1), true);
        }
        else {
            referenceNode = new QueryFieldReferenceNode(strRefNodeName, false);
        }
        return referenceNode;
    }

    public ElasticSqlQueryField parseSelectQueryField(SQLExpr queryFieldExpr, String queryAs) {
        if (queryFieldExpr instanceof SQLAllColumnExpr) {
            return ElasticSqlQueryFields.newMatchAllRootDocField();
        }
        QueryFieldReferencePath referencePath = buildQueryFieldRefPath(queryFieldExpr, queryAs);

        StringBuilder fullPathQueryFieldNameBuilder = new StringBuilder();
        for (QueryFieldReferenceNode referenceNode : referencePath.getReferenceNodes()) {
            fullPathQueryFieldNameBuilder.append(referenceNode.getReferenceNodeName());
            fullPathQueryFieldNameBuilder.append(FIELD_REF_PATH_DOT);
        }
        if (fullPathQueryFieldNameBuilder.length() > 0) {
            fullPathQueryFieldNameBuilder.deleteCharAt(fullPathQueryFieldNameBuilder.length() - 1);
        }

        return ElasticSqlQueryFields.newSqlSelectField(fullPathQueryFieldNameBuilder.toString());
    }

    public ElasticSqlQueryField parseConditionQueryField(SQLExpr queryFieldExpr, String queryAs) {
        QueryFieldReferencePath referencePath = buildQueryFieldRefPath(queryFieldExpr, queryAs);
        StringBuilder queryFieldPrefixBuilder = new StringBuilder();

        String longestNestedDocContextPrefix = StringUtils.EMPTY;
        String longestInnerDocContextPrefix = StringUtils.EMPTY;

        for (Iterator<QueryFieldReferenceNode> nodeIt = referencePath.getReferenceNodes().iterator(); nodeIt.hasNext(); ) {
            QueryFieldReferenceNode referenceNode = nodeIt.next();
            queryFieldPrefixBuilder.append(referenceNode.getReferenceNodeName());

            if (referenceNode.isNestedDocReference()) {
                longestNestedDocContextPrefix = queryFieldPrefixBuilder.toString();
            }

            if (nodeIt.hasNext()) {
                longestInnerDocContextPrefix = queryFieldPrefixBuilder.toString();
            }

            queryFieldPrefixBuilder.append(FIELD_REF_PATH_DOT);
        }
        if (queryFieldPrefixBuilder.length() > 0) {
            queryFieldPrefixBuilder.deleteCharAt(queryFieldPrefixBuilder.length() - 1);
        }

        String queryFieldFullRefPath = queryFieldPrefixBuilder.toString();

        //nested doc field
        if (StringUtils.isNotBlank(longestNestedDocContextPrefix)) {
            if (longestNestedDocContextPrefix.length() < queryFieldFullRefPath.length()) {
                String queryFieldName = queryFieldFullRefPath.substring(longestNestedDocContextPrefix.length() + 1);
                return ElasticSqlQueryFields.newNestedDocQueryField(longestNestedDocContextPrefix, queryFieldName);
            }
            throw new ElasticSql2DslException(String.format("[syntax error] nested doc field[%s] parse error!", queryFieldFullRefPath));
        }

        //root doc field
        if (referencePath.getReferenceNodes().size() == 1) {
            if (StringUtils.isNotBlank(queryAs) && queryAs.equalsIgnoreCase(queryFieldFullRefPath)) {
                throw new ElasticSql2DslException(String.format("[syntax error] ambiguous query field[%s], queryAs[%s]", queryFieldFullRefPath, queryAs));
            }
            return ElasticSqlQueryFields.newRootDocQueryField(queryFieldFullRefPath);
        }

        //inner doc field
        if (longestInnerDocContextPrefix.length() < queryFieldFullRefPath.length()) {
            String innerDocFieldName = queryFieldFullRefPath.substring(longestInnerDocContextPrefix.length() + 1);
            return ElasticSqlQueryFields.newInnerDocQueryField(longestInnerDocContextPrefix, innerDocFieldName);
        }
        throw new ElasticSql2DslException(String.format("[syntax error] query field[%s] parse error!", queryFieldFullRefPath));
    }

    private QueryFieldReferencePath buildQueryFieldRefPath(SQLExpr queryFieldExpr, String queryAs) {
        QueryFieldReferencePath referencePath = new QueryFieldReferencePath();

        List<QueryFieldReferenceNode> referenceNodeList = parseQueryFieldExprToRefPath(queryFieldExpr);
        if (CollectionUtils.isEmpty(referenceNodeList)) {
            throw new ElasticSql2DslException("[parse_query_field] referenceNodes is empty!");
        }
        QueryFieldReferenceNode firstRefNode = referenceNodeList.get(0);

        if (referenceNodeList.size() == 1) {
            referencePath.addReferenceNode(firstRefNode);
            return referencePath;
        }

        if (StringUtils.isNotBlank(queryAs) && !firstRefNode.isNestedDocReference() && queryAs.equalsIgnoreCase(firstRefNode.getReferenceNodeName())) {
            referenceNodeList.remove(0);
        }

        for (QueryFieldReferenceNode referenceNode : referenceNodeList) {
            referencePath.addReferenceNode(referenceNode);
        }

        return referencePath;
    }
}
