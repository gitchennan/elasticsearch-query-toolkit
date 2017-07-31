package org.es.mapping.mapper;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.es.mapping.annotations.fieldtype.DateField;
import org.es.mapping.utils.BeanUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;

public class DateFieldMapper {

    public static boolean isValidDateType(Field field) {
        if (BeanUtils.isCollectionType(field)) {
            if (!BeanUtils.isValidCollectionType(field)) {
                throw new IllegalArgumentException(
                        String.format("Unsupported list class type, name[%s].", field.getName()));
            }

            Class genericTypeClass = (Class) BeanUtils.getCollectionGenericType(field);
            return Date.class.isAssignableFrom(genericTypeClass);
        }

        Class<?> fieldClass = field.getType();
        return Date.class.isAssignableFrom(fieldClass);
    }

    public static void mapDataType(XContentBuilder mappingBuilder, DateField dateField) throws IOException {
        mappingBuilder.field("type", "date");
        mappingBuilder.field("format", dateField.format());

        if (dateField.boost() != 1.0f) {
            mappingBuilder.field("boost", dateField.boost());
        }

        if (!dateField.doc_values()) {
            mappingBuilder.field("doc_values", dateField.doc_values());
        }

        if (dateField.ignore_malformed()) {
            mappingBuilder.field("ignore_malformed", dateField.ignore_malformed());
        }

        if (!dateField.include_in_all()) {
            mappingBuilder.field("include_in_all", dateField.include_in_all());
        }
        else if (!dateField.index()) {
            mappingBuilder.field("include_in_all", false);
        }

        if (!dateField.index()) {
            mappingBuilder.field("index", dateField.index());
        }

        if (StringUtils.isNotBlank(dateField.null_value())) {
            mappingBuilder.field("null_value", dateField.null_value());
        }

        if (dateField.store()) {
            mappingBuilder.field("store", dateField.store());
        }

        if (!"ROOT".equalsIgnoreCase(dateField.locale())) {
            mappingBuilder.field("locale", dateField.locale());
        }
    }

    public static void mapDataType(XContentBuilder mappingBuilder, Field field) throws IOException {
        if (!isValidDateType(field)) {
            throw new IllegalArgumentException(
                    String.format("field type[%s] is invalid type of date.", field.getType()));
        }

        if (field.isAnnotationPresent(DateField.class)) {
            DateField dateField = field.getDeclaredAnnotation(DateField.class);
            mapDataType(mappingBuilder, dateField);
            return;
        }

        mappingBuilder.field("type", "date");
        mappingBuilder.field("format", "strict_date_optional_time||epoch_millis");
    }
}
