package org.elasticsearch.dsl.parser.helper;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.dsl.exception.ElasticSql2DslException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class ElasticSqlDateParseHelper {
    public static final String DEFAULT_ES_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    public static final Pattern SQL_DATE_REGEX_PATTERN_01 = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
    public static final Pattern SQL_DATE_REGEX_PATTERN_02 = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}");
    public static final Pattern SQL_DATE_REGEX_PATTERN_03 = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    private enum SqlDateRegex {
        DATE_REGEX_01 {
            @Override
            Pattern getPattern() {
                return SQL_DATE_REGEX_PATTERN_01;
            }

            @Override
            String getPatternString() {
                return "yyyy-MM-dd HH:mm:ss";
            }
        },
        DATE_REGEX_02 {
            @Override
            Pattern getPattern() {
                return SQL_DATE_REGEX_PATTERN_02;
            }

            @Override
            String getPatternString() {
                return "yyyy-MM-dd HH:mm";
            }
        },
        DATE_REGEX_03 {
            @Override
            Pattern getPattern() {
                return SQL_DATE_REGEX_PATTERN_03;
            }

            @Override
            String getPatternString() {
                return "yyyy-MM-dd";
            }

        };

        abstract Pattern getPattern();

        abstract String getPatternString();
    }

    public static boolean isDateMethod(SQLMethodInvokeExpr dateMethodExpr) {
        return "date".equalsIgnoreCase(dateMethodExpr.getMethodName());
    }

    public static boolean isDateArgStringValue(String date) {
        return SqlDateRegex.DATE_REGEX_01.getPattern().matcher(date).matches()
                || SqlDateRegex.DATE_REGEX_02.getPattern().matcher(date).matches()
                || SqlDateRegex.DATE_REGEX_03.getPattern().matcher(date).matches();
    }

    public static boolean isDateArgObjectValue(Object date) {
        if (date instanceof Date) {
            return true;
        }
        return false;
    }

    public static String formatDefaultEsDateStringValue(String date) {
        if (SqlDateRegex.DATE_REGEX_01.getPattern().matcher(date).matches()) {
            return formatDefaultEsDate(SqlDateRegex.DATE_REGEX_01.getPatternString(), date);
        }
        if (SqlDateRegex.DATE_REGEX_02.getPattern().matcher(date).matches()) {
            return formatDefaultEsDate(SqlDateRegex.DATE_REGEX_02.getPatternString(), date);
        }
        if (SqlDateRegex.DATE_REGEX_03.getPattern().matcher(date).matches()) {
            return formatDefaultEsDate(SqlDateRegex.DATE_REGEX_03.getPatternString(), date);
        }
        throw new ElasticSql2DslException("[syntax error] ElasticSql cannot support such date type: " + date);
    }

    public static String formatDefaultEsDateObjectValue(Object date) {
        if (date instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_ES_DATE_FORMAT);
            return dateFormat.format(date);
        }
        throw new ElasticSql2DslException("[syntax error] ElasticSql cannot support such date type: " + date.getClass());
    }

    public static String formatDefaultEsDate(String patternArg, String timeValArg) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(patternArg);
            Date date = dateFormat.parse(timeValArg);

            dateFormat = new SimpleDateFormat(DEFAULT_ES_DATE_FORMAT);
            return dateFormat.format(date);
        } catch (ParseException pex) {
            throw new ElasticSql2DslException("[syntax error] Parse time arg error: " + timeValArg);
        }
    }

    public static void checkDateMethod(SQLMethodInvokeExpr dateInvokeExpr) {
        if (!"date".equalsIgnoreCase(dateInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] ElasticSql not support method:" + dateInvokeExpr.getMethodName());
        }

        if (CollectionUtils.isEmpty(dateInvokeExpr.getParameters()) || dateInvokeExpr.getParameters().size() != 2) {
            throw new ElasticSql2DslException(String.format("[syntax error] There is no %s args method named nvl",
                    dateInvokeExpr.getParameters() != null ? dateInvokeExpr.getParameters().size() : 0));
        }

        SQLExpr patternArg = dateInvokeExpr.getParameters().get(0);
        SQLExpr timeValArg = dateInvokeExpr.getParameters().get(1);

        if (!(patternArg instanceof SQLCharExpr) && !(patternArg instanceof SQLVariantRefExpr)) {
            throw new ElasticSql2DslException("[syntax error] The first arg of date method should be a time pattern");
        }

        if (!(timeValArg instanceof SQLCharExpr) && !(timeValArg instanceof SQLVariantRefExpr)) {
            throw new ElasticSql2DslException("[syntax error] The second arg of date method should be a string of time");
        }
    }
}
