package com.tinkerpop.blueprints.impls.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Lukas Krejci
 * @since 1.0
 */
final class Statements {

    private final SqlGraph graph;

    public Statements(SqlGraph graph) {
        this.graph = graph;
    }

    public PreparedStatement getAddVertex() throws SQLException {
        String sql = "INSERT INTO vertices (id) VALUES (DEFAULT)";
        return graph.getConnection().prepareStatement(sql,
            Statement.RETURN_GENERATED_KEYS);
    }

    public PreparedStatement getAddEdge(long inVertexId, long outVertexId, String label) throws SQLException {
        String sql = "INSERT INTO edges (id, vertex_in, vertex_out, label) VALUES (DEFAULT, ?, ?, ?)";
        PreparedStatement stmt = graph.getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        stmt.setLong(1, inVertexId);
        stmt.setLong(2, outVertexId);
        stmt.setString(3, label);
        return stmt;
    }

    public PreparedStatement getGetEdge(long id) throws SQLException {
        String sql = "SELECT id, vertex_in, vertex_out, label FROM edges WHERE id = ?";
        PreparedStatement stmt = graph.getConnection().prepareStatement(sql);
        stmt.setLong(1, id);
        return stmt;
    }

    public PreparedStatement getGetVertex(long id) throws SQLException {
        String sql = "SELECT id FROM vertices WHERE id = ?";
        PreparedStatement stmt = graph.getConnection().prepareStatement(sql);
        stmt.setLong(1, id);
        return stmt;
    }

    public PreparedStatement getRemoveVertex(long id) throws SQLException {
        String sql = "DELETE FROM vertices WHERE id = ?";
        PreparedStatement stmt = graph.getConnection().prepareStatement(sql);
        stmt.setLong(1, id);
        return stmt;
    }

    public PreparedStatement getRemoveEdge(long id) throws SQLException {
        String sql = "DELETE FROM edges WHERE id = ?";
        PreparedStatement stmt = graph.getConnection().prepareStatement(sql);
        stmt.setLong(1, id);
        return stmt;
    }

    public PreparedStatement getAllVertices() throws SQLException {
        String sql = "SELECT id FROM vertices";
        return graph.getConnection()
            .prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    public PreparedStatement getAllEdges() throws SQLException {
        String sql = "SELECT id, vertex_in, vertex_out, label FROM edges";
        return graph.getConnection()
            .prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    public SqlVertex fromVertexResultSet(ResultSet rs) throws SQLException {
        if (!rs.next()) {
            return null;
        }

        return SqlVertex.GENERATOR.generate(graph, rs);
    }
}
