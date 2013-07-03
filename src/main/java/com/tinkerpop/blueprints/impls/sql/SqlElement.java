package com.tinkerpop.blueprints.impls.sql;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.impls.sql.utils.Encoder;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
abstract class SqlElement implements Element {

    protected final ThreadLocal<Connection> conn;
    protected final Object id;
    protected final SqlGraph graph;

    SqlElement(ThreadLocal<Connection> conn, SqlGraph graph, Object id) {
        this.id = (Long) id;
        this.conn = conn;
        this.graph = graph;
    }

    @Override
    public <T> T getProperty(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Property key cannot be null (or empty)");
        }

        try {
            String sql = "SELECT value FROM " + getPropTbl()
                    + " WHERE " + getFk() + " = ? AND name = ?";
            PreparedStatement stmt = conn.get().prepareStatement(sql);
            stmt.setLong(1, (Long) id);
            stmt.setString(2, key);
            ResultSet resultSet = stmt.executeQuery();
            try {
                if (resultSet.next()) {
                    return (T) Encoder.decodeValue(resultSet.getBytes(1));
                } else {
                    return null;
                }
            } finally {
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Set<String> getPropertyKeys() {
        try {
            Set<String> keys = new HashSet<String>();
            String sql = "SELECT name FROM " + getPropTbl() + " WHERE " + getFk() + " = ?";
            PreparedStatement stmt = conn.get().prepareStatement(sql);
            stmt.setLong(1, (Long) id);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                keys.add(resultSet.getString(1));
            }
            return keys;
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public void setProperty(String key, Object value) {
        ElementHelper.validateProperty(this, key, value);

        byte[] encoded = Encoder.encodeValue(value);

        try {
            // Ugh! 'Upsert' is complicated!
            // http://stackoverflow.com/a/6527838/285374
            // http://stackoverflow.com/a/8702291/285374
            // Update that will noop if the key doesn't exist...
            String update = "UPDATE " + getPropTbl() + " SET value = ?" +
                    " WHERE " + getFk() + "=? AND name = ?";
            PreparedStatement updateStmt = conn.get().prepareStatement(update);
            updateStmt.setBytes(1, encoded);
            updateStmt.setLong(2, (Long) id);
            updateStmt.setString(3, key);
            updateStmt.execute();

            // Insert that will noop if the key *does* exist
            PreparedStatement insertStmt = getNoopInsert(key, encoded);
            insertStmt.execute();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    private PreparedStatement getNoopInsert(String key, byte[] encoded)
            throws SQLException {
        // Sigh... can't find a no-op insert syntax that works for
        // both Postgres and Mysql...
        if (graph.getJdbcUrl().contains("mysql")) {
            String sql = "INSERT IGNORE INTO " + getPropTbl() + " (" + getFk() + ", name, value) VALUES (?,?,?)";
            PreparedStatement stmt = conn.get().prepareStatement(sql);
            stmt.setLong(1, (Long) id);
            stmt.setString(2, key);
            stmt.setBytes(3, encoded);
            return stmt;
        } else {
            String sql = "INSERT INTO " + getPropTbl() + " (" + getFk() + ", name, value)" +
                    " SELECT ?, ?, ?" +
                    " WHERE NOT EXISTS (SELECT 1 FROM " + getPropTbl() +
                    " WHERE " + getFk() + " = ? AND name = ? )";
            PreparedStatement stmt = conn.get().prepareStatement(sql);
            stmt.setLong(1, (Long) id);
            stmt.setString(2, key);
            stmt.setBytes(3, encoded);
            stmt.setLong(4, (Long) id);
            stmt.setString(5, key);
            return stmt;
        }
    }

    @Override
    public <T> T removeProperty(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Property key cannot be null");
        }
        T value = getProperty(key);
        String sql = "DELETE FROM " + getPropTbl() +
                " WHERE " + getFk() + "=? AND name = ?";
        try {
            PreparedStatement stmt = conn.get().prepareStatement(sql);
            stmt.setLong(1, (Long) id);
            stmt.setString(2, key);
            stmt.execute();
            return value;
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public void remove() {
        // CASCADE will remove the properties and edges.
        try {
            String sql = "DELETE FROM " + getTbl() + " WHERE id = ?";
            PreparedStatement stmt = conn.get().prepareStatement(sql);
            stmt.setLong(1, (Long) id);
            stmt.execute();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + id + "]";
    }

    @Override
    public Object getId() {
        return id;
    }

    abstract String getTbl();

    abstract String getPropTbl();

    abstract String getFk();

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlElement pgElement = (SqlElement) o;
        if (!id.equals(pgElement.id)) return false;
        return true;
    }
}
