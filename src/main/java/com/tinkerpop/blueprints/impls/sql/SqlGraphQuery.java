package com.tinkerpop.blueprints.impls.sql;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class SqlGraphQuery implements GraphQuery {
    @Override
    public GraphQuery has(String s) {
        return null;  // TODO: Implement
    }

    @Override
    public GraphQuery hasNot(String s) {
        return null;  // TODO: Implement
    }

    @Override
    public GraphQuery has(String s, Object o) {
        return null;  // TODO: Implement
    }

    @Override
    public GraphQuery hasNot(String s, Object o) {
        return null;  // TODO: Implement
    }

    @Override
    public GraphQuery has(String s, Predicate predicate, Object o) {
        return null;  // TODO: Implement
    }

    @Override
    public <T extends Comparable<T>> GraphQuery has(String s, T t, Compare compare) {
        return null;  // TODO: Implement
    }

    @Override
    public <T extends Comparable<T>> GraphQuery interval(String s, T t, T t2) {
        return null;  // TODO: Implement
    }

    @Override
    public GraphQuery limit(int i) {
        return null;  // TODO: Implement
    }

    @Override
    public Iterable<Edge> edges() {
        return null;  // TODO: Implement
    }

    @Override
    public Iterable<Vertex> vertices() {
        return null;  // TODO: Implement
    }
}
