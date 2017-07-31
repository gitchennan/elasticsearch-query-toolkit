package org.es.mapping.mapper;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.es.mapping.annotations.fieldtype.CompletionContext;
import org.es.mapping.annotations.fieldtype.CompletionField;
import org.es.mapping.utils.BeanUtils;


import java.io.IOException;
import java.lang.reflect.Field;

public class CompletionFieldMapper {

    public static boolean isValidCompletionFieldType(Field field) {
        if (BeanUtils.isCollectionType(field)) {
            if (!BeanUtils.isValidCollectionType(field)) {
                throw new IllegalArgumentException(
                        String.format("Unsupported list class type, name[%s].", field.getName()));
            }

            Class genericTypeClass = (Class) BeanUtils.getCollectionGenericType(field);
            return String.class.isAssignableFrom(genericTypeClass) && field.isAnnotationPresent(CompletionField.class);
        }

        Class<?> fieldClass = field.getType();
        return (String.class == fieldClass) && field.isAnnotationPresent(CompletionField.class);
    }

    public static void mapDataType(XContentBuilder mappingBuilder, CompletionField completionField) throws IOException {
        mappingBuilder.field("type", "completion");

        if (!"simple".equalsIgnoreCase(completionField.analyzer())) {
            mappingBuilder.field("analyzer", completionField.analyzer());
        }

        if (!"simple".equalsIgnoreCase(completionField.search_analyzer())) {
            mappingBuilder.field("search_analyzer", completionField.search_analyzer());
        }

        if (!completionField.preserve_separators()) {
            mappingBuilder.field("preserve_separators", completionField.preserve_separators());
        }

        if (!completionField.preserve_position_increments()) {
            mappingBuilder.field("preserve_position_increments", completionField.preserve_position_increments());
        }

        if (completionField.max_input_length() != 50) {
            mappingBuilder.field("max_input_length", completionField.max_input_length());
        }

        if (completionField.contexts().length > 0) {
            mappingBuilder.startArray("contexts");
            for (CompletionContext completionContext : completionField.contexts()) {
                mappingBuilder.field("name", completionContext.name());
                mappingBuilder.field("type", completionContext.type());

                if (StringUtils.isNotBlank(completionContext.path())) {
                    mappingBuilder.field("path", completionContext.path());
                }
            }
            mappingBuilder.endArray();
        }
    }

    public static void mapDataType(XContentBuilder mappingBuilder, Field field) throws IOException {
        if (!isValidCompletionFieldType(field)) {
            throw new IllegalArgumentException(
                    String.format("field type[%s] is invalid type of string.", field.getType()));
        }

        CompletionField completionField = field.getDeclaredAnnotation(CompletionField.class);
        mapDataType(mappingBuilder, completionField);
    }
}
