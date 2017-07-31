package org.es.mapping.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.es.mapping.annotations.fieldtype.BinaryField;

import java.io.IOException;
import java.lang.reflect.Field;

public class BinaryFieldMapper {

    public static boolean isValidBinaryType(Field field) {
        Class<?> fieldClass = field.getType();
        return (String.class == fieldClass) && field.isAnnotationPresent(BinaryField.class);
    }


    public static void mapDataType(XContentBuilder mappingBuilder, BinaryField booleanField) throws IOException {
        mappingBuilder.field("type", "binary");

        if (!booleanField.doc_values()) {
            mappingBuilder.field("doc_values", booleanField.doc_values());
        }

        if (booleanField.store()) {
            mappingBuilder.field("store", booleanField.store());
        }
    }

    public static void mapDataType(XContentBuilder mappingBuilder, Field field) throws IOException {
        if (!isValidBinaryType(field)) {
            throw new IllegalArgumentException(
                    String.format("field type[%s] is invalid type of binary.", field.getType()));
        }

        BinaryField binaryField = field.getDeclaredAnnotation(BinaryField.class);
        mapDataType(mappingBuilder, binaryField);
    }
}
