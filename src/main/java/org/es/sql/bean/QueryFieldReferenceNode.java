package org.es.sql.bean;

public class QueryFieldReferenceNode {

    private boolean isNestedDocReference;

    private String referenceNodeName;


    public QueryFieldReferenceNode(String referenceNodeName, boolean isNestedDocReference) {
        this.isNestedDocReference = isNestedDocReference;
        this.referenceNodeName = referenceNodeName;
    }

    public boolean isNestedDocReference() {
        return isNestedDocReference;
    }

    public String getReferenceNodeName() {
        return referenceNodeName;
    }
}
