package com.tinkerpop.blueprints.impls.sql;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

import java.io.*;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Date;

import org.h2.jdbcx.JdbcDataSource;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 */
public class SqlGraphTest extends GraphTest {

    /**
     * Hacky randomised string that is used to initialise
     * the graph directory. It is refreshed for each test
     * so that individual tests don't tread on each other,
     * but the name is still persistent for the lifetime of
     * that test.
     */
    private String graphName;

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    public void testVertexQueryTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexQueryTestSuite(this));
        printTestPerformance("VertexQueryTestSuite", this.stopWatch());
    }

    public void testGraphQueryTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphQueryTestSuite(this));
        printTestPerformance("GraphQueryTestSuite", this.stopWatch());
    }

    public void testTransactionalGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TransactionalGraphTestSuite(this));
        printTestPerformance("TransactionalGraphTestSuite", this.stopWatch());
    }

    public void testGraphMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphMLReaderTestSuite(this));
        printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
    }

    public void testGraphSONReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphSONReaderTestSuite(this));
        printTestPerformance("GraphSONReaderTestSuite", this.stopWatch());
    }

    public void testGMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GMLReaderTestSuite(this));
        printTestPerformance("GMLReaderTestSuite", this.stopWatch());
    }

    @Override
    public Graph generateGraph() {
        return generateGraph(graphName);
    }

    @Override
    public Graph generateGraph(String graphName) {
        String path = getGraphPath(graphName);
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:" + path);
        SqlGraph g = new SqlGraph(ds);
        try {
            g.createSchemaIfNeeded();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return g;
    }

    public void doTestSuite(final TestSuite testSuite) throws Exception {

        for (Method method : testSuite.getClass().getDeclaredMethods()) {
            String directory = this.getWorkingDirectory();
            deleteDirectory(new File(directory));
            if (method.getName().startsWith("test")) {
                graphName = "graph-" + (new Date().getTime());
                System.out.println("Testing " + method.getName() + "...");
                method.invoke(testSuite);
                deleteDirectory(new File(directory));
            }
        }
    }

    private String getGraphPath(String graphName) {
        return new File(getWorkingDirectory(), graphName).getPath();
    }

    private String getWorkingDirectory() {
        return this.computeTestDataRoot().getAbsolutePath();
    }
}
