package org.es.lu;

import com.google.gson.Gson;

public class IndexState {

    private String indexName;

    private IndexStatus indexStatus;

    private IndexState(String indexName, IndexStatus indexStatus) {
        this.indexName = indexName;
        this.indexStatus = indexStatus;
    }

    public enum IndexStatus {
        GREEN,
        YELLOW,
        RED,
        INDEX_MISSING,
        NO_NODE_AVAILABLE;

        public static IndexStatus fromString(String statusName) {
            for (IndexStatus indexStatus : IndexStatus.values()) {
                if (indexStatus.name().equalsIgnoreCase(statusName)) {
                    return indexStatus;
                }
            }
            return null;
        }
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexStatus getIndexStatus() {
        return indexStatus;
    }

    public boolean isWriteable() {
        return getIndexStatus() == IndexStatus.GREEN
                || getIndexStatus() == IndexStatus.YELLOW;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this, IndexState.class);
    }

    public static IndexState newIndexState(String index, IndexStatus indexStatus) {
        return new IndexState(index, indexStatus);
    }
}
