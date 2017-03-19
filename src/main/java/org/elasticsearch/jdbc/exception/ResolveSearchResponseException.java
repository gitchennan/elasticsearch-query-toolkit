package org.elasticsearch.jdbc.exception;


public class ResolveSearchResponseException extends RuntimeException {
    public ResolveSearchResponseException() {
    }

    public ResolveSearchResponseException(String message) {
        super(message);
    }

    public ResolveSearchResponseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ResolveSearchResponseException(Throwable cause) {
        super(cause);
    }

    public ResolveSearchResponseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
