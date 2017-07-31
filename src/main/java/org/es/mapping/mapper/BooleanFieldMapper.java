package org.es.mapping.mapper;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.es.mapping.annotations.fieldtype.BooleanField;
import org.es.mapping.utils.BeanUtils;


import java.io.IOException;
import java.lang.reflect.Field;

public class BooleanFieldMapper {

    public static boolean isValidBooleanType(Field field) {
        if (BeanUtils.isCollectionType(field)) {
            if (!BeanUtils.isValidCollectionType(field)) {
                throw new IllegalArgumentException(
                        String.format("Unsupported list class type, name[%s].", field.getName()));
            }

            Class genericTypeClass = (Class) BeanUtils.getCollectionGenericType(field);
            return Boolean.class == genericTypeClass || boolean.class == genericTypeClass;
        }

        Class<?> fieldClass = field.getType();
        return Boolean.class == fieldClass || boolean.class == fieldClass;
    }


    public static void mapDataType(XContentBuilder mappingBuilder, BooleanField booleanField) throws IOException {
        mappingBuilder.field("type", "boolean");
        if (booleanField.boost() != 1.0f) {
            mappingBuilder.field("boost", booleanField.boost());
        }

        if (!booleanField.doc_values()) {
            mappingBuilder.field("doc_values", booleanField.doc_values());
        }

        if (!booleanField.index()) {
            mappingBuilder.field("index", booleanField.index());
        }

        if (StringUtils.isNotBlank(booleanField.null_value())) {
            mappingBuilder.field("null_value", booleanField.null_value());
        }

        if (booleanField.store()) {
            mappingBuilder.field("store", booleanField.store());
        }
    }

    public static void mapDataType(XContentBuilder mappingBuilder, Field field) throws IOException {
        if (!isValidBooleanType(field)) {
            throw new IllegalArgumentException(
                    String.format("field type[%s] is invalid type of boolean.", field.getType()));
        }

        if (field.isAnnotationPresent(BooleanField.class)) {
            BooleanField booleanField = field.getDeclaredAnnotation(BooleanField.class);
            mapDataType(mappingBuilder, booleanField);
            return;
        }

        mappingBuilder.field("type", "boolean");
    }
}
