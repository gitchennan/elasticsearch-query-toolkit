package org.es.sql.bean;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;

public class QueryFieldReferencePath {

    private List<QueryFieldReferenceNode> referenceNodes;

    public void addReferenceNode(QueryFieldReferenceNode referenceNode) {
        if (referenceNodes == null) {
            referenceNodes = Lists.newLinkedList();
        }
        referenceNodes.add(referenceNode);
    }

    public List<QueryFieldReferenceNode> getReferenceNodes() {
        if (CollectionUtils.isEmpty(referenceNodes)) {
            return Collections.emptyList();
        }
        return ImmutableList.copyOf(referenceNodes);
    }
}
