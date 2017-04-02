package org.es.jdbc.exception;


public class BuildElasticClientException extends RuntimeException {
    public BuildElasticClientException() {
    }

    public BuildElasticClientException(String message) {
        super(message);
    }

    public BuildElasticClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public BuildElasticClientException(Throwable cause) {
        super(cause);
    }

    public BuildElasticClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
