package org.es.mapping.annotations.enums;

public enum IndexOptions {
    Default {
        @Override
        public String code() {
            return "none";
        }
    },

    /**
     * Only the doc number is indexed. Can answer the question Does this term exist in this field?
     */
    Docs {
        @Override
        public String code() {
            return "docs";
        }
    },

    /**
     * Doc number and term frequencies are indexed.
     * Term frequencies are used to score repeated terms higher than single terms.
     */
    Freqs {
        @Override
        public String code() {
            return "freqs";
        }
    },

    /**
     * Doc number, term frequencies, and term positions (or order) are indexed.
     * Positions can be used for proximity or phrase queries.
     */
    Positions {
        @Override
        public String code() {
            return "positions";
        }
    },

    /**
     * Doc number, term frequencies, positions,
     * and start and end character offsets (which map the term back to the original string) are indexed.
     * Offsets are used by the postings highlighter.
     */
    Offsets {
        @Override
        public String code() {
            return "offsets";
        }
    };

    public abstract String code();
}
