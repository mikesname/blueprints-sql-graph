package com.tinkerpop.blueprints.impls.sql;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class SqlEdgeIterable<T extends Edge> implements CloseableIterable<Edge> {
    private final ResultSet rs;
    private final ThreadLocal<Connection> conn;
    private final SqlGraph graph;

    SqlEdgeIterable(final ThreadLocal<Connection> conn, SqlGraph graph, final ResultSet rs) {
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
    public Iterator<Edge> iterator() {
        try {
            // Reset the ResultSet each time we ask for
            // an iterable.
            rs.beforeFirst();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
        return new Iterator<Edge>() {
            private Edge nextVal = null;

            @Override
            public boolean hasNext() {
                if (nextVal != null) {
                    // Already been cached.
                    return true;
                }
                try {
                    if (rs.next()) {
                        nextVal = new SqlEdge(conn, graph,
                                rs.getLong(1),
                                rs.getLong(2),
                                rs.getLong(3),
                                rs.getString(4));
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
            public Edge next() {
                if (nextVal != null) {
                    Edge next = nextVal;
                    nextVal = null;
                    return next;
                } else {
                    try {
                        if (rs.next()) {
                            return new SqlEdge(conn, graph,
                                    rs.getLong(1),
                                    rs.getLong(2),
                                    rs.getLong(3),
                                    rs.getString(4));
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
