package org.es.mapping.mapper;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.es.mapping.annotations.fieldtype.TokenCountField;

import java.io.IOException;

public class TokenCountFieldMapper {

    public static void mapDataType(XContentBuilder mappingBuilder, TokenCountField tokenCountField) throws IOException {
        mappingBuilder.field("type", "token_count");
        mappingBuilder.field("analyzer", tokenCountField.analyzer());

        if (!tokenCountField.enable_position_increments()) {
            mappingBuilder.field("enable_position_increments", tokenCountField.enable_position_increments());
        }

        if (tokenCountField.boost() != 1.0f) {
            mappingBuilder.field("boost", tokenCountField.boost());
        }

        if (!tokenCountField.doc_values()) {
            mappingBuilder.field("doc_values", tokenCountField.doc_values());
        }

        if (!tokenCountField.include_in_all()) {
            mappingBuilder.field("include_in_all", tokenCountField.include_in_all());
        }
        else if (!tokenCountField.index()) {
            mappingBuilder.field("include_in_all", false);
        }

        if (!tokenCountField.index()) {
            mappingBuilder.field("index", tokenCountField.index());
        }

        if (StringUtils.isNotBlank(tokenCountField.null_value())) {
            mappingBuilder.field("null_value", tokenCountField.null_value());
        }

        if (tokenCountField.store()) {
            mappingBuilder.field("store", tokenCountField.store());
        }
    }
}
