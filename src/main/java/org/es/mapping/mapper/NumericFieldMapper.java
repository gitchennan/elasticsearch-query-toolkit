package org.es.mapping.mapper;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.es.mapping.annotations.enums.NumberType;
import org.es.mapping.annotations.fieldtype.NumberField;
import org.es.mapping.utils.BeanUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

public class NumericFieldMapper {

    private static final Map<Class, NumberType> validNumericFieldClassMap = ImmutableMap.<Class, NumberType>builder()
            .put(Double.class, NumberType.Double)
            .put(BigDecimal.class, NumberType.Double)
            .put(Float.class, NumberType.Float)
            .put(Long.class, NumberType.Long)
            .put(BigInteger.class, NumberType.Long)
            .put(Integer.class, NumberType.Integer)
            .put(Short.class, NumberType.Short)
            .put(Byte.class, NumberType.Byte)

            .put(double.class, NumberType.Double)
            .put(float.class, NumberType.Float)
            .put(long.class, NumberType.Long)
            .put(int.class, NumberType.Integer)
            .put(short.class, NumberType.Short)
            .put(byte.class, NumberType.Byte)
            .build();

    public static boolean isValidNumberType(Field field) {
        if (BeanUtils.isCollectionType(field)) {
            if (!BeanUtils.isValidCollectionType(field)) {
                throw new IllegalArgumentException(
                        String.format("Unsupported more than one collection generic type, name[%s].", field.getName()));
            }

            Class genericTypeClass = (Class) BeanUtils.getCollectionGenericType(field);
            return validNumericFieldClassMap.keySet().contains(genericTypeClass);
        }


        Class<?> fieldClass = field.getType();
        return validNumericFieldClassMap.keySet().contains(fieldClass);
    }

    public static void mapDataType(XContentBuilder mappingBuilder, NumberField numberField) throws IOException {
        mappingBuilder.field("type", numberField.type().code());

        if (!numberField.coerce()) {
            mappingBuilder.field("coerce", numberField.coerce());
        }

        if (numberField.boost() != 1.0f) {
            mappingBuilder.field("boost", numberField.boost());
        }

        if (!numberField.doc_values()) {
            mappingBuilder.field("doc_values", numberField.doc_values());
        }

        if (numberField.ignore_malformed()) {
            mappingBuilder.field("ignore_malformed", numberField.ignore_malformed());
        }

        if (!numberField.include_in_all()) {
            mappingBuilder.field("include_in_all", numberField.include_in_all());
        }
        else if (!numberField.index()) {
            mappingBuilder.field("include_in_all", false);
        }

        if (!numberField.index()) {
            mappingBuilder.field("index", numberField.index());
        }

        if (StringUtils.isNotBlank(numberField.null_value())) {
            mappingBuilder.field("null_value", numberField.null_value());
        }

        if (numberField.store()) {
            mappingBuilder.field("store", numberField.store());
        }

        if (numberField.type() == NumberType.ScaledFloat && numberField.scaling_factor() != 1) {
            mappingBuilder.field("scaling_factor", numberField.scaling_factor());
        }
    }

    public static void mapDataType(XContentBuilder mappingBuilder, Field field) throws IOException {
        if (!isValidNumberType(field)) {
            throw new IllegalArgumentException(
                    String.format("field type[%s] is invalid type of number.", field.getType()));
        }

        if (field.isAnnotationPresent(NumberField.class)) {
            NumberField numberField = field.getDeclaredAnnotation(NumberField.class);
            mapDataType(mappingBuilder, numberField);
            return;
        }

        if (BeanUtils.isCollectionType(field)) {
            Class genericTypeClass = (Class) BeanUtils.getCollectionGenericType(field);
            NumberType numberType = validNumericFieldClassMap.get(genericTypeClass);
            mappingBuilder.field("type", numberType.code());
        }
        else {
            NumberType numberType = validNumericFieldClassMap.get(field.getType());
            mappingBuilder.field("type", numberType.code());
        }

    }
}
