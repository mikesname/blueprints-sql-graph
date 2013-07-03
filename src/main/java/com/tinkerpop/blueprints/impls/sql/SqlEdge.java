package com.tinkerpop.blueprints.impls.sql;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

import java.sql.Connection;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class SqlEdge extends SqlElement implements Edge {

    public static final String TABLE_NAME = "edges";
    public static final String PROPERTY_TABLE_NAME = "edge_properties";
    public static final String PROPERTY_TABLE_COL = "edge_id";

    private final Object vertexOut;
    private final Object vertexIn;
    private final String label;

    SqlEdge(ThreadLocal<Connection> conn, SqlGraph graph, Object id, Object vertexOut, Object vertexIn, String label) {
        super(conn, graph, id);
        this.vertexIn = vertexIn;
        this.vertexOut = vertexOut;
        this.label = label;
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        switch (direction) {
            case IN: return new SqlVertex(conn, graph, vertexIn);
            case OUT: return new SqlVertex(conn, graph, vertexOut);
            default: throw new IllegalArgumentException("Direction must be either IN or OUT, not BOTH");
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + id + " (" + label + ")]";
    }

    @Override
    public void setProperty(String key, Object value) {
        if (key != null && key.equals(StringFactory.LABEL)) {
            throw new IllegalArgumentException("Property key '" + StringFactory.LABEL + "' is reserved.");
        }
        super.setProperty(key, value);
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    String getTbl() {
        return TABLE_NAME;
    }

    @Override
    String getPropTbl() {
        return PROPERTY_TABLE_NAME;
    }

    @Override
    String getFk() {
        return PROPERTY_TABLE_COL;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SqlEdge pgEdge = (SqlEdge) o;

        if (!label.equals(pgEdge.label)) return false;
        if (!vertexIn.equals(pgEdge.vertexIn)) return false;
        if (!vertexOut.equals(pgEdge.vertexOut)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + vertexOut.hashCode();
        result = 31 * result + vertexIn.hashCode();
        result = 31 * result + label.hashCode();
        return result;
    }
}
