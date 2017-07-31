package org.es.mapping.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {

    public final static String EMPTY = "";

    public static boolean equals(Object arg1, Object arg2) {
        if (arg1 == null || arg2 == null) {
            return false;
        }
        return arg1.toString().equals(arg2.toString());
    }


    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isNotBlank(String str) {
        return !StringUtils.isBlank(str);
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    public static Boolean isNumeric(String... params) {
        Pattern pattern = Pattern.compile("^[0-9]+$");
        for (String param : params) {
            if (StringUtils.isEmpty(param)) {
                return false;
            }
            Matcher matcher = pattern.matcher(param);
            if (!matcher.matches()) {
                return false;
            }
        }
        return true;
    }

    public static boolean isAllBlank(String... params) {
        for (String param : params) {
            if (!StringUtils.isBlank(param)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isAllNotBlank(String... params) {
        for (String param : params) {
            if (StringUtils.isBlank(param)) {
                return false;
            }
        }
        return true;
    }
}
