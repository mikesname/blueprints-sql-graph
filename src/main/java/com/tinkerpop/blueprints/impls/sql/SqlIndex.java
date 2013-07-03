package com.tinkerpop.blueprints.impls.sql;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class SqlIndex<T extends Element> implements Index<T> {
    @Override
    public String getIndexName() {
        return null;  // TODO: Implement
    }

    @Override
    public Class<T> getIndexClass() {
        return null;  // TODO: Implement
    }

    @Override
    public void put(String s, Object o, T t) {
        // TODO: Implement
    }

    @Override
    public CloseableIterable<T> get(String s, Object o) {
        return null;  // TODO: Implement
    }

    @Override
    public CloseableIterable<T> query(String s, Object o) {
        return null;  // TODO: Implement
    }

    @Override
    public long count(String s, Object o) {
        return 0;  // TODO: Implement
    }

    @Override
    public void remove(String s, Object o, T t) {
        // TODO: Implement
    }
}
