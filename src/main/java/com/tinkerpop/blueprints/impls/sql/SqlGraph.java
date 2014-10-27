package com.tinkerpop.blueprints.impls.sql;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.WeakHashMap;

import javax.sql.DataSource;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

import org.apache.commons.configuration.Configuration;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 * @author Lukas Krejci
 * @since 1.0
 */
public final class SqlGraph implements ThreadedTransactionalGraph {
    private static final Features FEATURES = new Features();

    static {
        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = false;
        FEATURES.supportsUniformListProperty = false;
        FEATURES.supportsMixedListProperty = false;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = true;

        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.isWrapper = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = false;
        FEATURES.supportsEdgeIndex = false;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsTransactions = true;
        FEATURES.supportsIndices = false;
        FEATURES.supportsKeyIndices = false;
        FEATURES.supportsVertexKeyIndex = false;
        FEATURES.supportsEdgeKeyIndex = false;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = true;
        FEATURES.supportsThreadIsolatedTransactions = false;
    }

    private final DataSource dataSource;
    private volatile Connection connection;
    private volatile Statements statements;
    private final String verticesTableName;
    private final String edgesTableName;
    private final String vertexPropertiesTableName;
    private final String edgePropertiesTableName;

    private final WeakHashMap<Long, WeakReference<SqlVertex>> vertexCache = new WeakHashMap<>();

    public SqlGraph(Configuration configuration) throws Exception {
        String dsClass = configuration.getString("sql.datasourceClass");
        dataSource = (DataSource) Class.forName(dsClass).newInstance();
        verticesTableName = configuration.getString("sql.verticesTable", "vertices");
        edgesTableName = configuration.getString("sql.edgesTable", "edges");
        vertexPropertiesTableName = configuration.getString("sql.vertexPropertiesTable", "vertex_properties");
        edgePropertiesTableName = configuration.getString("sql.edgePropertiesTable", "edge_properties");
    }

    public SqlGraph(DataSource dataSource) {
        this(dataSource, null);
    }

    private SqlGraph(DataSource dataSource, Connection connection) {
        this.dataSource = dataSource;
        this.connection = connection;
        verticesTableName = "vertices";
        edgesTableName = "edges";
        vertexPropertiesTableName = "vertex_properties";
        edgePropertiesTableName = "edge_properties";
    }

    public void createSchemaIfNeeded() throws SQLException, IOException {
        ensureConnection();

        try (Statement st = connection.createStatement()) {
            st.execute("SELECT 1 FROM " + getVerticesTableName());
            return;
        } catch (SQLException ignored) {
            //good, the schema doesn't exist. Let's continue
        }

        String dbName = connection.getMetaData().getDatabaseProductName();
        String script = dbName + "-schema.sql";
        InputStream schemaStream = getClass().getClassLoader().getResourceAsStream(script);
        if (schemaStream == null) {
            schemaStream = getClass().getClassLoader().getResourceAsStream("schema.sql");
        }

        if (schemaStream == null) {
            throw new AssertionError("Could not load the schema creation script.");
        }

        String contents = null;
        try (InputStreamReader rdr = new InputStreamReader(schemaStream)) {
            StringBuilder bld = new StringBuilder();
            char[] buffer = new char[512];

            int cnt;
            while ((cnt = rdr.read(buffer)) != -1) {
                bld.append(buffer, 0, cnt);
            }

            contents = bld.toString();
        }

        contents = contents.replace("%VERTICES%", verticesTableName);
        contents = contents.replace("%VERTEX_PROPERTIES%", vertexPropertiesTableName);
        contents = contents.replace("%EDGES%", edgesTableName);
        contents = contents.replace("%EDGE_PROPERTIES%", edgePropertiesTableName);

        String[] inst = contents.split(";");
        Statement st = connection.createStatement();

        for (int i = 0; i < inst.length; i++) {
            // we ensure that there is no spaces before or after the request string
            // in order to not execute empty statements
            if (!inst[i].trim().equals("")) {
                st.executeUpdate(inst[i]);
            }
        }
    }

    @Override
    public TransactionalGraph newTransaction() {
        try {
            Connection conn = newConnection();
            return new SqlGraph(dataSource, conn);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public void stopTransaction(Conclusion conclusion) {
        if (conclusion == Conclusion.SUCCESS) {
            commit();
        } else {
            rollback();
        }
    }

    @Override
    public void commit() {
        try {
            ensureConnection();
            connection.commit();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public void rollback() {
        try {
            ensureConnection();
            connection.rollback();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    @Override
    public Vertex addVertex(Object id) {
        ensureConnection();
        try (PreparedStatement stmt = statements.getAddVertex()) {
            if (stmt.executeUpdate() == 0) {
                return null;
            }
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                return cache(statements.fromVertexResultSet(rs));
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public SqlVertex getVertex(Object id) {
        Long realId = getId(id);

        if (realId == null) {
            return null;
        }

        WeakReference<SqlVertex> ref = vertexCache.get(realId);

        SqlVertex v = ref == null ? null : ref.get();
        if (v != null) {
            return v;
        }

        ensureConnection();
        try (PreparedStatement stmt = statements.getGetVertex(realId)) {
            if (!stmt.execute()) {
                return null;
            }

            try (ResultSet rs = stmt.getResultSet()) {
                return cache(statements.fromVertexResultSet(rs));
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public void removeVertex(Vertex vertex) {
        ensureConnection();
        try (PreparedStatement stmt = statements.getRemoveVertex((Long) vertex.getId())) {
            if (stmt.executeUpdate() == 0) {
                throw new IllegalStateException("Vertex with id " + vertex.getId() + " doesn't exist.");
            }
            vertexCache.remove(vertex.getId());
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public CloseableIterable<Vertex> getVertices() {
        ensureConnection();
        try {
            PreparedStatement stmt = statements.getAllVertices();

            if (!stmt.execute()) {
                stmt.close();
                return ResultSetIterable.empty();
            }

            return new ResultSetIterable<Vertex>(SqlVertex.GENERATOR, this, stmt.getResultSet());
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public CloseableIterable<Vertex> getVertices(String key, Object value) {
        return query().has(key, value).vertices();
    }

    @Override
    public SqlEdge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        if (label == null) {
            throw new IllegalArgumentException("null label");
        }

        ensureConnection();

        try (PreparedStatement stmt = statements
            .getAddEdge((Long) inVertex.getId(), (Long) outVertex.getId(), label)) {

            if (stmt.executeUpdate() == 0) {
                return null;
            }

            long eid = -1;
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (!rs.next()) {
                    return null;
                }

                eid = rs.getLong(1);
            }

            try (ResultSet rs = statements.getGetEdge(eid).executeQuery()) {
                if (!rs.next()) {
                    return null;
                }

                return SqlEdge.GENERATOR.generate(this, rs);
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public SqlEdge getEdge(Object id) {
        Long eid = getId(id);
        if (eid == null) {
            return null;
        }

        ensureConnection();

        try (PreparedStatement stmt = statements.getGetEdge(eid)) {
            if (!stmt.execute()) {
                return null;
            }

            try (ResultSet rs = stmt.getResultSet()) {
                if (!rs.next()) {
                    return null;
                }
                return SqlEdge.GENERATOR.generate(this, rs);
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public void removeEdge(Edge edge) {
        ensureConnection();

        try (PreparedStatement stmt = statements.getRemoveEdge((Long) edge.getId())) {
            if (stmt.executeUpdate() == 0) {
                throw new IllegalStateException("Edge with id " + edge.getId() + " doesn't exist.");
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Iterable<Edge> getEdges() {
        ensureConnection();

        try {
            PreparedStatement stmt = statements.getAllEdges();
            return new ResultSetIterable<Edge>(SqlEdge.GENERATOR, this, stmt.executeQuery());
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public CloseableIterable<Edge> getEdges(String key, Object value) {
        return query().has(key, value).edges();
    }

    @Override
    public SqlGraphQuery query() {
        return new SqlGraphQuery(this);
    }

    @Override
    public void shutdown() {
        if (connection != null) {
            try {
                connection.commit();
                connection.close();
            } catch (SQLException e) {
                throw new SqlGraphException(e);
            }
        }
    }

    @Override
    public String toString() {
        return "sqlgraph(" + dataSource.toString() + ")";
    }

    Connection getConnection() {
        ensureConnection();
        return connection;
    }

    Statements getStatements() {
        ensureConnection();
        return statements;
    }

    String getVerticesTableName() {
        return verticesTableName;
    }

    String getEdgesTableName() {
        return edgesTableName;
    }

    String getVertexPropertiesTableName() {
        return vertexPropertiesTableName;
    }

    String getEdgePropertiesTableName() {
        return edgePropertiesTableName;
    }

    private void ensureConnection() {
        if (connection == null) {
            try {
                connection = newConnection();
                statements = new Statements(this);
            } catch (SQLException e) {
                throw new SqlGraphException(e);
            }
        }
    }

    private Connection newConnection() throws SQLException {
        Connection conn = dataSource.getConnection();
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return conn;
    }

    private Long getId(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("null id");
        } else if (id instanceof String) {
            try {
                return Long.parseLong((String) id);
            } catch (NumberFormatException e) {
                return null;
            }
        } else if (id instanceof Long) {
            return (Long) id;
        } else {
            return null;
        }
    }
    private SqlVertex cache(SqlVertex v) {
        if (v != null) {
            vertexCache.put(v.getId(), new WeakReference<>(v));
        }

        return v;
    }
}
