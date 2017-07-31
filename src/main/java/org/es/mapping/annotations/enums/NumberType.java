package org.es.mapping.annotations.enums;

/**
 * As far as integer types (byte, short, integer and long) are concerned,
 * you should pick the smallest type which is enough for your use-case.
 * This will help indexing and searching be more efficient. Note however that given that
 * storage is optimized based on the actual values that are stored, picking one type over
 * another one will have no impact on storage requirements.
 * <p/>
 * For floating-point types, it is often more efficient to store floating-point data into an integer using a scaling factor,
 * which is what the scaled_float type does under the hood. For instance, a price field could be
 * stored in a scaled_float with a scaling_factor of 100. All APIs would work as if the field was stored as a double,
 * but under the hood elasticsearch would be working with the number of cents, price*100, which is an integer.
 * This is mostly helpful to save disk space since integers are way easier to compress than floating points.
 * scaled_float is also fine to use in order to trade accuracy for disk space.
 * For instance imagine that you are tracking cpu utilization as a number between 0 and 1.
 * It usually does not matter much whether cpu utilization is 12.7% or 13%,
 * so you could use a scaled_float with a scaling_factor of 100 in order to round cpu utilization to the closest percent in order to save space.
 * <p/>
 * If scaled_float is not a good fit, then you should pick the smallest type that is enough for the use-case
 * among the floating-point types: double, float and half_float. Here is a table that compares these types in order to help make a decision.
 *
 * @author chennan
 */
public enum NumberType {
    /**
     * A signed 64-bit integer with a minimum value of -2^63 and a maximum value of 2^63-1.
     */
    Long {
        @Override
        public String code() {
            return "long";
        }
    },

    /**
     * A signed 32-bit integer with a minimum value of -2^31 and a maximum value of 2^31-1.
     */
    Integer {
        @Override
        public String code() {
            return "integer";
        }
    },

    /**
     * A signed 16-bit integer with a minimum value of -32,768 and a maximum value of 32,767.
     */
    Short {
        @Override
        public String code() {
            return "short";
        }
    },

    /**
     * A signed 8-bit integer with a minimum value of -128 and a maximum value of 127.
     */
    Byte {
        @Override
        public String code() {
            return "byte";
        }
    },

    /**
     * A double-precision 64-bit IEEE 754 floating point
     */
    Double {
        @Override
        public String code() {
            return "double";
        }
    },

    /**
     * A single-precision 32-bit IEEE 754 floating point.
     */
    Float {
        @Override
        public String code() {
            return "float";
        }
    },

    /**
     * A half-precision 16-bit IEEE 754 floating point.
     */
    HalfFloat {
        @Override
        public String code() {
            return "half_float";
        }
    },

    /**
     * A floating point that is backed by a long and a fixed scaling factor.
     */
    ScaledFloat {
        @Override
        public String code() {
            return "scaled_float";
        }
    };

    public abstract String code();
}
