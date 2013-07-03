package com.tinkerpop.blueprints.impls.sql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.sql.utils.Encoder;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Properties;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class SqlGraph implements TransactionalGraph {

    private final ComboPooledDataSource dataSource;
    private final boolean ownsDataSource;
    private final boolean autoBuildSchema;

    public SqlGraph(ComboPooledDataSource dataSource) {
        this.dataSource = dataSource;
        this.ownsDataSource = false;
        this.autoBuildSchema = false;
        checkIfNeedsSchema();
    }

    public SqlGraph(String driverClass, String jdbcUrl, String user, String password, boolean autoSchema) {
        try {
            // FIXME: Set system props to shut up C3P0 logging...
            Properties p = new Properties(System.getProperties());
            p.put("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");
            p.put("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "OFF");
            System.setProperties(p);

            dataSource = new ComboPooledDataSource();
            dataSource.setDriverClass(driverClass);
            dataSource.setJdbcUrl(jdbcUrl);
            if (user != null)
                dataSource.setUser(user);
            if (password != null)
                dataSource.setPassword(password);
            ownsDataSource = true;
            autoBuildSchema = autoSchema;
            checkIfNeedsSchema();
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
    }

    public SqlGraph(String driverClass, String jdbcUrl, boolean autoSchema) {
        this(driverClass, jdbcUrl, null, null, autoSchema);
    }

    public SqlGraph(String driverClass, String jdbcUrl) {
        this(driverClass, jdbcUrl, null, null, driverClass.contentEquals("org.h2.Driver"));
    }

    protected final ThreadLocal<Connection> conn = new ThreadLocal<Connection>() {
        protected Connection initialValue() {
            try {
                Connection conn = dataSource.getConnection();
                conn.setAutoCommit(false);
                conn.setTransactionIsolation(
                        Connection.TRANSACTION_READ_UNCOMMITTED);
                return conn;
            } catch (SQLException e) {
                throw new SqlGraphException(e);
            }
        }

        @Override
        public Connection get() {
            Connection conn = super.get();
            try {
                if (conn.isClosed()) {
                    return initialValue();
                } else {
                    return conn;
                }
            } catch (SQLException e) {
                throw new SqlGraphException(e);
            }
        }
    };

    private static final Features FEATURES = new Features();

    static {
        FEATURES.supportsSerializableObjectProperty = true;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = true;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = true;
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
        FEATURES.supportsEdgeRetrieval = false;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = false;
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    @Override
    public Vertex addVertex(Object o) {
        try {
            String sql = "INSERT INTO vertices (id) VALUES (DEFAULT)";
            PreparedStatement stmt = conn.get().prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS);
            stmt.execute();
            ResultSet resultSet = stmt.getGeneratedKeys();
            try {
                if (resultSet.next()) {
                    return new SqlVertex(conn, this, resultSet.getLong(1));
                }
                throw new RuntimeException("Unable to get id of inserted vertex");
            } finally {
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Vertex getVertex(Object id) {
        Long lid = getLongId(id);
        if (lid == null)
            return null;

        try {
            String sql = "SELECT id FROM vertices WHERE id = ?";
            PreparedStatement stmt = conn.get().prepareStatement(sql);
            stmt.setLong(1, lid);
            ResultSet resultSet = stmt.executeQuery();
            try {
                if (resultSet.next()) {
                    Long oid = resultSet.getLong(1);
                    return new SqlVertex(conn, this, oid);
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
    public void removeVertex(Vertex vertex) {
        vertex.remove();
    }

    @Override
    public CloseableIterable<Vertex> getVertices() {
        try {
            String sql = "SELECT * FROM vertices";
            Statement stmt = conn.get().createStatement(
                    ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            return new SqlVertexIterable<Vertex>(conn, this, stmt.executeQuery(sql));
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        byte[] encoded = Encoder.encodeValue(value);
        String sql = "SELECT vertex_id FROM vertex_properties WHERE name = ? AND value = ?";
        try {
            PreparedStatement stmt = conn.get().prepareStatement(sql,
                    ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            stmt.setString(1, key);
            stmt.setBytes(2, encoded);
            ResultSet resultSet = stmt.executeQuery();
            return new SqlVertexIterable<SqlVertex>(conn, this, resultSet);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Edge addEdge(Object o, Vertex out, Vertex in, String label) {
        return out.addEdge(label, in);
    }

    @Override
    public Edge getEdge(Object id) {
        if (null == id)
            throw ExceptionFactory.edgeIdCanNotBeNull();

        Long lid = getLongId(id);
        if (lid == null)
            return null;

        String sql = "SELECT id, vertex_out, vertex_in, label FROM edges WHERE id = ?";
        try {
            PreparedStatement stmt = conn.get().prepareStatement(sql);
            stmt.setLong(1, lid);
            ResultSet resultSet = stmt.executeQuery();
            try {
                if (resultSet.next()) {
                    return new SqlEdge(conn, this,
                            resultSet.getObject(1),
                            resultSet.getObject(2),
                            resultSet.getObject(3),
                            resultSet.getString(4));
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
    public void removeEdge(Edge edge) {
        edge.remove();
    }

    @Override
    public CloseableIterable<Edge> getEdges() {
        try {
            String sql = "SELECT * FROM edges";
            Statement stmt = conn.get().createStatement(
                    ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            return new SqlEdgeIterable<Edge>(conn, this, stmt.executeQuery(sql));
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        byte[] encoded = Encoder.encodeValue(value);
        try {
            String sql = "SELECT DISTINCT e.id, e.vertex_out, e.vertex_in, e.label" +
                    " FROM edges e, edge_properties ep" +
                    " WHERE e.id = ep.edge_id AND name = ? AND value = ?";
            PreparedStatement stmt = conn.get().prepareStatement(sql,
                    ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            stmt.setString(1, key);
            stmt.setBytes(2, encoded);
            ResultSet resultSet = stmt.executeQuery();
            return new SqlEdgeIterable<SqlEdge>(conn, this, resultSet);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public GraphQuery query() {
        return new DefaultGraphQuery(this);
    }

    @Override
    public void shutdown() {
        // Since the user supplied the connection, we
        // shouldn't close it here... but it should
        // commit any running tx...
        commit();
        if (ownsDataSource)
            dataSource.close();
    }

    @Override
    public void commit() {
        try {
            conn.get().commit();
            conn.get().close();
            conn.remove();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public void rollback() {
        try {
            conn.get().rollback();
            conn.get().close();
            conn.remove();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    @Deprecated
    public void stopTransaction(Conclusion conclusion) {
        if (conclusion.equals(Conclusion.FAILURE))
            rollback();
        else
            commit();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().toLowerCase() + " [" + dataSource.getJdbcUrl() + "]";
    }

    private Long getLongId(Object id) {
        Long lid;
        if (id == null) {
            throw new IllegalArgumentException("Vertex id cannot be null");
        }
        try {
            if (id instanceof String) {
                lid = Long.parseLong((String) id);
            } else if (id instanceof Integer) {
                lid = (long) (Integer) id;
            } else if (id instanceof Long) {
                lid = (Long) id;
            } else {
                lid = (Long) id;
            }
        } catch (ClassCastException e) {
            lid = null;
        } catch (NumberFormatException e) {
            lid = null;
        }
        return lid;
    }

    // Utilities

    private void checkIfNeedsSchema() {
        try {
            conn.get().createStatement().executeQuery("SELECT 1 FROM vertices");
        } catch (SQLException e) {
            if (e.getSQLState().startsWith("42")) { // Base table not found...
                try {
                    if (autoBuildSchema)
                        buildSchema();
                    else
                        System.err.println("Warning: no schema found. Use graph.buildSchema() if you really" +
                                " want to use this database.");
                } catch (SQLException e1) {
                    throw new SqlGraphException(e);
                } catch (IOException e1) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            try {
                conn.get().commit();
            } catch (SQLException e) {
                throw new SqlGraphException(e);
            }
        }

    }

    public void buildSchema() throws SQLException, IOException {
        String resource = dataSource.getJdbcUrl().contains("mysql") ?
                "mysql-schema.sql" : "schema.sql";
        buildSchema(conn.get(), resource);
    }

    /**
     * Hacks needed for SQL compat! Urgh...
     * @return
     */
    public String getJdbcUrl() {
        return dataSource.getJdbcUrl();
    }

    public static void buildSchema(Connection conn, String schemaResource) throws SQLException, IOException {

        InputStream stream = SqlGraph.class.getClassLoader()
                .getResourceAsStream(schemaResource);
        try {
            loadSqlFromInputStream(conn, stream);
        } finally {
            stream.close();
        }
    }

    public static void loadSqlFromInputStream(Connection conn, InputStream inputStream) throws SQLException {
        String s;
        StringBuilder sb = new StringBuilder();

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            while ((s = br.readLine()) != null) {
                sb.append(s);
            }
            br.close();
            String[] inst = sb.toString().split(";");
            Statement st = conn.createStatement();

            for (int i = 0; i < inst.length; i++) {
                // we ensure that there is no spaces before or after the request string
                // in order to not execute empty statements
                if (!inst[i].trim().equals("")) {
                    st.executeUpdate(inst[i]);
                }
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
