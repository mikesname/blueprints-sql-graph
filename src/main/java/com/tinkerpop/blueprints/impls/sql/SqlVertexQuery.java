package com.tinkerpop.blueprints.impls.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Contains;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * @author Lukas Krejci
 * @since 1.0
 */
public final class SqlVertexQuery implements VertexQuery {

    private final SqlGraph graph;
    private final long rootVertexId;
    private final QueryFilters filters = new QueryFilters();
    private int limit = -1;
    private Direction direction = Direction.OUT;

    public SqlVertexQuery(SqlGraph graph, long rootVertexId) {
        this.graph = graph;
        this.rootVertexId = rootVertexId;
    }

    @Override
    public SqlVertexQuery direction(Direction direction) {
        this.direction = direction;
        return this;
    }

    @Override
    public SqlVertexQuery labels(String... labels) {
        has("label", Contains.IN, Arrays.asList(labels));
        return this;
    }

    @Override
    public long count() {
        try (PreparedStatement stmt = generateCountEdgeQuery()) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return 0;
                }

                long cnt = rs.getLong(1);

                if (limit < 0) {
                    return cnt;
                } else {
                    return cnt > limit ? limit : cnt;
                }
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Object vertexIds() {
        return null; // TODO what's this?
    }

    @Override
    public SqlVertexQuery has(String key) {
        filters.has(key);
        return this;
    }

    @Override
    public SqlVertexQuery hasNot(String key) {
        filters.hasNot(key);
        return this;
    }

    @Override
    public SqlVertexQuery has(String key, Object value) {
        filters.has(key, value);
        return this;
    }

    @Override
    public SqlVertexQuery hasNot(String key, Object value) {
        filters.hasNot(key, value);
        return this;
    }

    @Override
    public SqlVertexQuery has(String key, Predicate predicate, Object value) {
        filters.has(key, predicate, value);
        return this;
    }

    @Override
    public <T extends Comparable<T>> VertexQuery has(String key, T value, Compare compare) {
        filters.has(key, value, compare);
        return this;
    }

    @Override
    public <T extends Comparable<?>> VertexQuery interval(String key, T startValue, T endValue) {
        filters.interval(key, startValue, endValue);
        return this;
    }

    @Override
    public VertexQuery limit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public CloseableIterable<Edge> edges() {
        try {
            PreparedStatement stmt = generateEdgeQuery();
            return new ResultSetIterable<Edge>(SqlEdge.GENERATOR, graph, stmt.executeQuery());
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public CloseableIterable<Vertex> vertices() {
        try {
            PreparedStatement stmt = generateVertexQuery();
            long artificialLimit = direction == Direction.BOTH ? limit : -1;
            return new ResultSetIterable<Vertex>(SqlVertex.GENERATOR, graph, stmt.executeQuery(), artificialLimit);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    private PreparedStatement generateCountEdgeQuery() throws SQLException {
        return generateQuery("SELECT COUNT(*)");
    }

    private PreparedStatement generateEdgeQuery() throws SQLException {
        return generateQuery("SELECT id, vertex_in, vertex_out, label");
    }

    private PreparedStatement generateVertexQuery() throws SQLException {
        String select;
        switch (direction) {
        case IN:
            select = "SELECT vertex_out";
            return generateQuery(select);
        case OUT:
            select = "SELECT vertex_in";
            return generateQuery(select);
        case BOTH:
            direction = Direction.IN;
            QueryFilters.SqlAndParams sql = generateQueryString("SELECT vertex_out");
            direction = Direction.OUT;
            QueryFilters.SqlAndParams sql2 = generateQueryString("SELECT vertex_in");
            direction = Direction.BOTH;

            sql.sql.append(" UNION ALL ").append(sql2.sql);
            sql.params.addAll(sql2.params);

            return generateQuery(sql);
        default:
            throw new IllegalStateException("unknown direction value");
        }

    }

    private PreparedStatement generateQuery(String select) throws SQLException {
        return generateQuery(generateQueryString(select));
    }

    private PreparedStatement generateQuery(QueryFilters.SqlAndParams sql) throws SQLException {
        PreparedStatement stmt = graph.getConnection()
            .prepareStatement(sql.sql.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        int i = 1;
        for (Object p : sql.params) {
            stmt.setObject(i++, p);
        }

        return stmt;
    }

    private QueryFilters.SqlAndParams generateQueryString(String select) throws SQLException {
        String edges = graph.getEdgesTableName();

        String directionFilter = null;
        switch (direction) {
        case IN:
            directionFilter = edges + ".vertex_in = ?";
            break;
        case OUT:
            directionFilter = edges + ".vertex_out = ?";
            break;
        case BOTH:
            directionFilter = "(" + edges + ".vertex_in = ? OR " + edges + ".vertex_out = ?)";
            break;
        }

        QueryFilters.SqlAndParams sql = filters.generateStatement(select, graph.getEdgesTableName(),
            graph.getEdgePropertiesTableName(),
            SqlEdge.getPropertyTableForeignKey(), SqlEdge.DISALLOWED_PROPERTY_NAMES, directionFilter);


        if (limit >= 0) {
            sql.sql.append(" LIMIT ").append(limit);
        }

        sql.params.add(0, rootVertexId);
        if (direction == Direction.BOTH) {
            sql.params.add(0, rootVertexId);
        }

        return sql;
    }
}
