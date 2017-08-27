package org.es.sql.utils;


import org.slf4j.LoggerFactory;

public class Logger {
    public static void warn(Object obj, String message, Throwable e) {

        org.slf4j.Logger log = LoggerFactory.getLogger(obj.getClass().getName());
        if (log.isWarnEnabled()) {
            if (e == null) {
                log.warn(message);
            }
            else {
                log.warn(message, e);
            }
        }
    }

    public static void warn(Object obj, String message) {
        warn(obj, message, null);
    }

    public static void error(Object obj, String message) {
        error(obj.getClass(), message, null);
    }

    public static void error(Class clz, String message) {
        error(clz, message, null);
    }

    public static void error(Object obj, String message, Throwable e) {
        org.slf4j.Logger log = LoggerFactory.getLogger(obj instanceof Class ? ((Class) obj).getName() : obj.getClass().getName());
        if (log.isErrorEnabled()) {
            if (e == null) {
                log.error(message);
            }
            else {
                log.error(message, e);
            }
        }
    }

    public static void info(Object obj, String message) {
        info(obj.getClass(), message);
    }

    public static void info(Class clz, String message) {
        org.slf4j.Logger log = LoggerFactory.getLogger(clz.getName());
        if (log.isInfoEnabled()) {
            log.info(message);
        }
    }

    public static void infoAndTrace(Object obj, String message) {
        infoAndTrace(obj.getClass(), message);
    }

    public static void infoAndTrace(Class clz, String message) {
        info(clz, message);

        trace(clz, message);
    }

    private static void trace(Class clz, String message) {
        org.slf4j.Logger log = LoggerFactory.getLogger(clz.getName());
        if (log.isTraceEnabled()) {
            log.trace(message);
        }
    }

    public static void debug(Object obj, String message) {
        debug(obj.getClass(), message);
    }

    public static void debug(Class clz, String message) {
        org.slf4j.Logger log = LoggerFactory.getLogger(clz.getName());
        if (log.isDebugEnabled()) {
            log.debug(message);
        }
    }
}
