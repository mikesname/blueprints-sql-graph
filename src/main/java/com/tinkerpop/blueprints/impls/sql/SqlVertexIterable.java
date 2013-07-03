package com.tinkerpop.blueprints.impls.sql;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Vertex;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class SqlVertexIterable<T extends Vertex> implements CloseableIterable<Vertex> {

    private final ResultSet rs;
    private final ThreadLocal<Connection> conn;
    private final SqlGraph graph;

    SqlVertexIterable(final ThreadLocal<Connection> conn, SqlGraph graph, final ResultSet rs) {
        this.conn = conn;
        this.rs = rs;
        this.graph = graph;
    }

    @Override
    public void close() {
        try {
            rs.close();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Iterator<Vertex> iterator() {
        try {
            // Reset the ResultSet each time we ask for
            // an iterable.
            rs.beforeFirst();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
        return new Iterator<Vertex>() {
            private Object nextVal = null;

            @Override
            public boolean hasNext() {

                if (nextVal != null) {
                    // Already been cached.
                    return true;
                }

                try {
                    if (rs.next()) {
                        nextVal = rs.getLong(1);
                        return true;
                    } else {
                        nextVal = null;
                        return false;
                    }
                } catch (SQLException e) {
                    throw new SqlGraphException(e);
                }
            }

            @Override
            public SqlVertex next() {
                if (nextVal != null) {
                    SqlVertex vertex = new SqlVertex(conn, graph, nextVal);
                    nextVal = null;
                    return vertex;
                } else {
                    try {
                        if (rs.next()) {
                            return new SqlVertex(conn, graph, rs.getLong(1));
                        } else {
                            throw new NoSuchElementException();
                        }
                    } catch (SQLException e) {
                        throw new SqlGraphException(e);
                    }
                }
            }

            @Override
            public void remove() {
                throw new NotImplementedException();
            }
        };
    }
}
