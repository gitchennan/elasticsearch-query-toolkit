package org.es.mapping.annotations.enums;


public enum TermVector {
    /**
     * No term vectors are stored. (default)
     */
    No {
        @Override
        public String code() {
            return "code";
        }
    },

    /**
     * Just the terms in the field are stored.
     */
    Yes {
        @Override
        public String code() {
            return "yes";
        }
    },

    /**
     * Terms and positions are stored.
     */
    WithPositions {
        @Override
        public String code() {
            return "with_positions";
        }
    },
    /**
     * Terms and character offsets are stored.
     */
    WithOffsets {
        @Override
        public String code() {
            return "with_offsets";
        }
    },

    /**
     * Terms, positions, and character offsets are stored.
     */
    WithPositionsOffsets {
        @Override
        public String code() {
            return "with_positions_offsets";
        }
    };

    public abstract String code();
}
