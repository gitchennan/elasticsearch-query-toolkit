package org.elasticsearch.dsl;

public class ElasticSqlIdentifier {

    private String pathName;
    private String propertyName;
    private IdentifierType identifierType;

    public ElasticSqlIdentifier(String propertyName) {
        this.propertyName = propertyName;
        this.identifierType = IdentifierType.Property;
    }

    public ElasticSqlIdentifier(String pathName, String propertyName, IdentifierType identifierType) {
        if (identifierType != IdentifierType.InnerDocProperty && identifierType != IdentifierType.NestedDocProperty && identifierType != IdentifierType.MatchAllField) {
            throw new IllegalArgumentException("identifierType must one of [InnerDocProperty, NestedDocProperty]");
        }
        this.pathName = pathName;
        this.propertyName = propertyName;
        this.identifierType = identifierType;
    }

    public String getPathName() {
        return pathName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public IdentifierType getIdentifierType() {
        return identifierType;
    }

    public static enum IdentifierType {
        MatchAllField,
        Property,
        InnerDocProperty,
        NestedDocProperty,
    }
}
