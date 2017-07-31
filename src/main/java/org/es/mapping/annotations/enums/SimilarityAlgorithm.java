package org.es.mapping.annotations.enums;

public enum SimilarityAlgorithm {
    Default {
        @Override
        public String code() {
            return "BM25";
        }
    },
    BM25 {
        @Override
        public String code() {
            return "BM25";
        }
    };

    public abstract String code();
}
