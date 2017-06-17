package org.es.sql.parser.sql.sort;

import org.elasticsearch.search.sort.FieldSortBuilder;

public interface ConditionSortBuilder {
    FieldSortBuilder buildSort(String idfName);
}