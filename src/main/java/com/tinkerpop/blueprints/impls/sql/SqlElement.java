package com.tinkerpop.blueprints.impls.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tinkerpop.blueprints.Element;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 * @author Lukas Krejci
 * @since 1.0
 */
abstract class SqlElement implements Element {

    protected final SqlGraph graph;
    private final Long id;

    protected SqlElement(SqlGraph graph, Long id) {
        if (id == null) {
            throw new IllegalArgumentException("id can't be null");
        }

        this.graph = graph;
        this.id = id;
    }

    protected abstract String getPropertiesTableName();

    protected abstract String getPropertyTableElementIdName();

    protected abstract List<String> getDisallowedPropertyNames();
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProperty(String key) {
        String sql = "SELECT string_value, numeric_value, value_type FROM " + getPropertiesTableName() + " WHERE " +
            getPropertyTableElementIdName() + " = ? AND name = ?";

        try (PreparedStatement stmt = graph.getConnection().prepareStatement(sql)) {
            stmt.setLong(1, id);
            stmt.setString(2, key);

            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }

                int valueTypeInt = rs.getInt(3);
                ValueType valueType = ValueType.values()[valueTypeInt];

                return (T) valueType.convertFromDBType(rs.getObject(valueType.isNumeric() ? 2 : 1));
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Set<String> getPropertyKeys() {
        String sql =
            "SELECT name FROM " + getPropertiesTableName() + " WHERE " + getPropertyTableElementIdName() + " = ?";

        try (PreparedStatement stmt = graph.getConnection().prepareStatement(sql)) {
            stmt.setLong(1, id);

            Set<String> ret = new HashSet<>();

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ret.add(rs.getString(1));
                }
            }

            return ret;
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public void setProperty(String key, Object value) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("empty key");
        }

        if (getDisallowedPropertyNames().contains(key)) {
            throw new IllegalArgumentException("disallowed property name");
        }

        if (value == null) {
            throw new IllegalArgumentException("null value not allowed");
        }

        ValueType valueType = ValueType.of(value, true);
        if (valueType == null) {
            throw new IllegalArgumentException(
                "Unsupported value type " + value.getClass() + ". Only primitive types and string are supported.");
        }

        String sql = "UPDATE " + getPropertiesTableName() + " SET " +
            (valueType.isNumeric() ? "numeric_value" : "string_value") + " = ?, value_type = ? WHERE " +
            getPropertyTableElementIdName() + " = ? AND name = ?";

        try (PreparedStatement stmt = graph.getConnection().prepareStatement(sql)) {
            stmt.setObject(1, value);
            stmt.setInt(2, valueType.ordinal());
            stmt.setLong(3, id);
            stmt.setString(4, key);

            if (stmt.executeUpdate() == 0) {
                sql = "INSERT INTO " + getPropertiesTableName() + " (" + getPropertyTableElementIdName() +
                    ", name, string_value, numeric_value, value_type) VALUES (?, ?, ?, ?, ?)";

                try (PreparedStatement stmt2 = graph.getConnection().prepareStatement(sql)) {
                    stmt2.setLong(1, id);
                    stmt2.setString(2, key);
                    stmt2.setObject(3, valueType.isNumeric() ? null : value);
                    stmt2.setObject(4, valueType.isNumeric() ? value : null);
                    stmt2.setInt(5, valueType.ordinal());

                    stmt2.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public <T> T removeProperty(String key) {
        T value = getProperty(key);

        String sql = "DELETE FROM " + getPropertiesTableName() + " WHERE " + getPropertyTableElementIdName() + " = ? AND name = ?";
        try (PreparedStatement stmt = graph.getConnection().prepareStatement(sql)) {
            stmt.setLong(1, id);
            stmt.setString(2, key);
            stmt.executeUpdate();
            return value;
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SqlElement sqlVertex = (SqlElement) o;

        if (!id.equals(sqlVertex.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("[id=").append(id);
        sb.append(']');
        return sb.toString();
    }

}
