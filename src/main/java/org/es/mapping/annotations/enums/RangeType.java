package org.es.mapping.annotations.enums;

public enum RangeType {
    /**
     * A range of signed 32-bit integers with a minimum value of -2^31 and maximum of 2^31-1.
     */
    IntegerRange {
        @Override
        public String code() {
            return "integer_range";
        }
    },

    /**
     * A range of single-precision 32-bit IEEE 754 floating point values.
     */
    FloatRange {
        @Override
        public String code() {
            return "float_range";
        }
    },

    /**
     * A range of signed 64-bit integers with a minimum value of -263 and maximum of 263-1.
     */
    LongRange {
        @Override
        public String code() {
            return "long_range";
        }
    },

    /**
     * A range of double-precision 64-bit IEEE 754 floating point values.
     */
    DoubleRange {
        @Override
        public String code() {
            return "double_range";
        }
    },

    /**
     * A range of date values represented as unsigned 64-bit integer milliseconds elapsed since system epoch.
     */
    DateRange {
        @Override
        public String code() {
            return "date_range";
        }
    };

    public abstract String code();
}
