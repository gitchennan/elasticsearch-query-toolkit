package org.es.mapping.mapper;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.es.mapping.annotations.fieldtype.IPField;


import java.io.IOException;
import java.lang.reflect.Field;

public class IPFieldMapper {

    public static boolean isValidIPFieldType(Field field) {
        Class<?> fieldClass = field.getType();
        return String.class.isAssignableFrom(fieldClass) && field.isAnnotationPresent(IPField.class);
    }

    public static void mapDataType(XContentBuilder mappingBuilder, IPField ipField) throws IOException {
        mappingBuilder.field("type", "ip");

        if (ipField.boost() != 1.0f) {
            mappingBuilder.field("boost", ipField.boost());
        }

        if (!ipField.doc_values()) {
            mappingBuilder.field("doc_values", ipField.doc_values());
        }

        if (!ipField.include_in_all()) {
            mappingBuilder.field("include_in_all", ipField.include_in_all());
        }
        else if (!ipField.index()) {
            mappingBuilder.field("include_in_all", false);
        }

        if (!ipField.index()) {
            mappingBuilder.field("index", ipField.index());
        }

        if (StringUtils.isNotBlank(ipField.null_value())) {
            mappingBuilder.field("null_value", ipField.null_value());
        }

        if (ipField.store()) {
            mappingBuilder.field("store", ipField.store());
        }
    }

    public static void mapDataType(XContentBuilder mappingBuilder, Field field) throws IOException {
        if (!isValidIPFieldType(field)) {
            throw new IllegalArgumentException(
                    String.format("field type[%s] is invalid type of ip.", field.getType()));
        }

        IPField ipField = field.getDeclaredAnnotation(IPField.class);
        mapDataType(mappingBuilder, ipField);
    }
}
