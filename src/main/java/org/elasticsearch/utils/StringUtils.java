package org.elasticsearch.utils;

public class StringUtils {
    public static boolean isBlank(String src) {
        if (src == null) {
            return true;
        }
        int len = src.length();
        for (int idx = 0; idx < len; idx++) {
            if (' ' != src.charAt(idx)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isNotBlank(String src) {
        return !isBlank(src);
    }
}
