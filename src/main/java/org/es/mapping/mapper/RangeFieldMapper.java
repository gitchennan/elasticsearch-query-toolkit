package org.es.mapping.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.es.mapping.annotations.enums.RangeType;
import org.es.mapping.annotations.fieldtype.RangeField;

import java.io.IOException;
import java.lang.reflect.Field;

public class RangeFieldMapper {

    public static boolean isValidRangeFieldType(Field field) {
        return field.isAnnotationPresent(RangeField.class);
    }

    public static void mapDataType(XContentBuilder mappingBuilder, RangeField rangeField) throws IOException {
        mappingBuilder.field("type", rangeField.type().code());

        if (rangeField.type() == RangeType.DateRange) {
            mappingBuilder.field("format", rangeField.format());
        }

        if (!rangeField.coerce()) {
            mappingBuilder.field("coerce", rangeField.coerce());
        }

        if (rangeField.boost() != 1.0f) {
            mappingBuilder.field("boost", rangeField.boost());
        }

        if (!rangeField.include_in_all()) {
            mappingBuilder.field("include_in_all", rangeField.include_in_all());
        }
        else if (!rangeField.index()) {
            mappingBuilder.field("include_in_all", false);
        }

        if (!rangeField.index()) {
            mappingBuilder.field("index", rangeField.index());
        }

        if (rangeField.store()) {
            mappingBuilder.field("store", rangeField.store());
        }

    }

    public static void mapDataType(XContentBuilder mappingBuilder, Field field) throws IOException {
        if (!isValidRangeFieldType(field)) {
            throw new IllegalArgumentException(
                    String.format("field type[%s] is invalid type of range.", field.getType()));
        }
        RangeField stringField = field.getDeclaredAnnotation(RangeField.class);
        mapDataType(mappingBuilder, stringField);
    }
}
