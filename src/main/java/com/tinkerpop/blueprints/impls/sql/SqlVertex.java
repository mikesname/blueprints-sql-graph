package com.tinkerpop.blueprints.impls.sql;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class SqlVertex extends SqlElement implements Vertex {

    public static final String TABLE_NAME = "vertices";
    public static final String PROPERTY_TABLE_NAME = "vertex_properties";
    public static final String PROPERTY_TABLE_COL = "vertex_id";

    SqlVertex(ThreadLocal<Connection> conn, SqlGraph graph, Object id) {
        super(conn, graph, id);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        try {
            PreparedStatement stmt = buildEdgeQueryStatement(direction, labels);
            ResultSet resultSet = stmt.executeQuery();
            try {
                // Since we're not returning a CloseableIterable we have to
                // store the results in memory.
                List<Edge> edges = new ArrayList<Edge>();
                while (resultSet.next()) {
                    Long eid = resultSet.getLong(1);
                    Long outV = resultSet.getLong(2);
                    Long inV = resultSet.getLong(3);
                    String label = resultSet.getString(4);
                    edges.add(new SqlEdge(conn, graph, eid, outV, inV, label));
                }
                return edges;
            } finally {
                resultSet.close();
            }

        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        try {
            PreparedStatement stmt = buildEdgeQueryStatement(direction, labels);
            ResultSet resultSet = stmt.executeQuery();
            try {
                // Since we're not returning a CloseableIterable we have to
                // store the results in memory.
                // NB: Probably a better way to filter duplicates than
                // using a HashSet.
                List<Vertex> vertices = new ArrayList<Vertex>();
                while (resultSet.next()) {
                    Long eid = resultSet.getLong(1);
                    Long outV = resultSet.getLong(2);
                    Long inV = resultSet.getLong(3);
                    String label = resultSet.getString(4);
                    switch (direction) {
                        case IN:
                            vertices.add(new SqlVertex(conn, graph, outV));
                            break;
                        case OUT:
                            vertices.add(new SqlVertex(conn, graph, inV));
                            break;
                        default: {
                            // If the source and target vertex are the same (i.e.
                            // it points to itself, the current vertex should be
                            // in the result set. Otherwise it shouldn't.
                            if (inV.equals(outV)) {
                                vertices.add(this);
                            } else {
                                if (!inV.equals(id)) {
                                    vertices.add(new SqlVertex(conn, graph, inV));
                                }
                                if (!outV.equals(id)) {
                                    vertices.add(new SqlVertex(conn, graph, outV));
                                }
                            }
                        }
                    }
                }
                if (direction.equals(Direction.BOTH))
                    System.out.println(vertices);
                return vertices;
            } finally {
                resultSet.close();
            }

        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    @Override
    public Edge addEdge(String label, Vertex vertex) {
        if (label == null)
            throw ExceptionFactory.edgeLabelCanNotBeNull();

        if (vertex == null) {
            throw new IllegalArgumentException("Vertex for addEdge is null");
        }
        try {
            String sql = "INSERT INTO edges (vertex_out, vertex_in, label) VAlUES (?, ?, ?)";
            PreparedStatement stmt = conn.get().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            stmt.setLong(1, (Long) id);
            stmt.setLong(2, (Long) vertex.getId());
            stmt.setString(3, label);
            stmt.execute();
            ResultSet resultSet = stmt.getGeneratedKeys();
            try {
                if (resultSet.next()) {
                    Long key = resultSet.getLong(1);
                    return new SqlEdge(conn, graph, key, id, vertex.getId(), label);
                }
            } finally {
                resultSet.close();
            }
            throw new RuntimeException("Unable to fetch last-inserted edge ID.");
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }

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

    private PreparedStatement buildEdgeQueryStatement(Direction direction, String[] labels) {
        String labelClause = "";
        if (labels.length == 1) {
            labelClause = " AND label=?";
        } else if (labels.length > 1) {
            labelClause = " AND label IN (?";
            for (int i = 1; i < labels.length; i++) labelClause += ", ?";
            labelClause += ")";
        }

        String sql = "SELECT id, vertex_out, vertex_in, label FROM edges WHERE ";
        switch (direction) {
            case IN:
                sql = sql + ("vertex_in=? " + labelClause);
                break;
            case OUT:
                sql = sql + ("vertex_out=? " + labelClause);
                break;
            default:
                sql = sql + ("vertex_out=? " + labelClause + " UNION ALL " + sql + "vertex_in=? " + labelClause);
        }

        try {
            PreparedStatement stmt = conn.get().prepareStatement(sql);
            stmt.setLong(1, (Long) id);
            int paramNum = 2;
            for (int i = paramNum; i < paramNum + labels.length; i++) {
                stmt.setString(i, labels[i - paramNum]);
            }
            paramNum += labels.length;
            if (direction.equals(Direction.BOTH)) {
                stmt.setLong(paramNum, (Long) id);
                paramNum++;
                for (int i = paramNum; i < paramNum + labels.length; i++) {
                    stmt.setString(i, labels[i - paramNum]);
                }
            }
            return stmt;
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }
}
