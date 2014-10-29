package com.tinkerpop.blueprints.impls.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.tinkerpop.blueprints.CloseableIterable;

/**
 * @author Lukas Krejci
 * @since 1.0
 */
class ResultSetIterable<T> implements CloseableIterable<T> {
    private final ElementGenerator<? extends T> generator;
    private final SqlGraph graph;
    private final ResultSet rs;
    private final long artificialLimit;

    private ResultSetIterable() {
        generator = null;
        graph = null;
        rs = null;
        artificialLimit = -1;
    }

    ResultSetIterable(ElementGenerator<? extends T> generator, SqlGraph graph, ResultSet rs) {
        this(generator, graph, rs, -1);
    }

    ResultSetIterable(ElementGenerator<? extends T> generator, SqlGraph graph, ResultSet rs, long artificialLimit) {
        this.generator = generator;
        this.graph = graph;
        this.rs = rs;
        this.artificialLimit = artificialLimit;
    }

    public static <T> ResultSetIterable<T> empty() {
        return new ResultSetIterable<T>() {
            @Override
            public void close() {
            }

            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public T next() {
                        throw new NoSuchElementException();
                    }

                    @Override
                    public void remove() {
                        throw new IllegalStateException();
                    }
                };
            }
        };
    }

    @Override
    public Iterator<T> iterator() {
        try {
            rs.beforeFirst();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }

        return new Iterator<T>() {

            T next;
            long cnt;

            @Override
            public boolean hasNext() {
                if (artificialLimit >= 0 && cnt >= artificialLimit) {
                    return false;
                }
                advance();
                return next != null;
            }

            @Override
            public T next() {
                try {
                    advance();
                    return next;
                } finally {
                    next = null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void advance() {
                if (next == null) {
                    try {
                        if (rs.next()) {
                            next = generator.generate(graph, rs);
                            cnt++;
                        }
                    } catch (SQLException e) {
                        throw new SqlGraphException(e);
                    }
                }
            }
        };
    }

    @Override
    public void close() {
        try {
            rs.close();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    protected void finalize () throws Throwable {
        close();
        super.finalize();
    }
}
