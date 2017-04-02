package org.es.sql.dsl.enums;

public enum SortOption {
    SUM {
        @Override
        public String mode() {
            return "sum";
        }
    },
    MIN {
        @Override
        public String mode() {
            return "min";
        }
    },
    MAX {
        @Override
        public String mode() {
            return "max";
        }
    },
    AVG {
        @Override
        public String mode() {
            return "avg";
        }
    };

    public static SortOption get(String mode) {
        SortOption op = null;
        for (SortOption option : SortOption.values()) {
            if (option.mode().equalsIgnoreCase(mode)) {
                op = option;
            }
        }
        return op;
    }

    public abstract String mode();

    @Override
    public String toString() {
        return mode();
    }
}