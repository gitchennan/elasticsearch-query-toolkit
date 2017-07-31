package org.es.mapping.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.es.mapping.annotations.fieldtype.PercolatorField;

import java.io.IOException;
import java.lang.reflect.Field;

public class PercolatorFieldMapper {

    public static boolean isValidPercolatorFieldType(Field field) {
        Class<?> fieldClass = field.getType();
        return String.class.isAssignableFrom(fieldClass) && field.isAnnotationPresent(PercolatorField.class);
    }

    public static void mapDataType(XContentBuilder mappingBuilder, Field field) throws IOException {
        if (!isValidPercolatorFieldType(field)) {
            throw new IllegalArgumentException(
                    String.format("field type[%s] is invalid type of percolator.", field.getType()));
        }

        mappingBuilder.field("type", "percolator");
    }
}
