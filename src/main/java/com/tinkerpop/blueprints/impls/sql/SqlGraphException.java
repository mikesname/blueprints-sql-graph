package com.tinkerpop.blueprints.impls.sql;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 * @author Lukas Krejci
 * @since 1.0
 */
public class SqlGraphException extends RuntimeException {

    public SqlGraphException() {
    }

    public SqlGraphException(String message) {
        super(message);
    }

    public SqlGraphException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlGraphException(Throwable cause) {
        super(cause);
    }

    public SqlGraphException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
