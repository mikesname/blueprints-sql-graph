package com.tinkerpop.blueprints.impls.sql;

import java.sql.SQLException;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class SqlGraphException extends RuntimeException {
    public SqlGraphException(SQLException ex) {
        super(ex);
    }
}
