package com.tinkerrocks.structure;

import com.tinkerrocks.storage.StorageConstants;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.IntStream;


/**
 * <p>
 * Test class
 * </p>
 * Created by ashishn on 8/11/15.
 */
public class RocksTest {
    private RocksGraph graph;

    @Before
    public void setup() throws IOException, InstantiationException {

        String path = "/tmp/databases_1";

        Configuration configuration = new BaseConfiguration();
        configuration.setProperty(StorageConstants.STORAGE_DIR_PROPERTY, path);
        FileUtils.deleteDirectory(new File(path));
        FileUtils.forceMkdir(new File(path));
        graph = RocksGraph.open(configuration);


    }


    @Test
    public void testMultiValues() {
        Vertex marko = graph.addVertex(T.label, "person", T.id, "jumbaho", "name", "marko", "age", 29);

        graph.createIndex("country", Vertex.class);

        marko.property(VertexProperty.Cardinality.list, "country", "usa");
        marko.property(VertexProperty.Cardinality.set, "country", "uk");
        marko.property(VertexProperty.Cardinality.set, "country", "uk");
        marko.property(VertexProperty.Cardinality.list, "country", "japan");
        GraphTraversalSource g = graph.traversal(GraphTraversalSource.build().engine(StandardTraversalEngine.build()));
        System.out.println(g.V("jumbaho").has("country", P.within("usa1", "usa2", "uk")).properties().toList());
    }


    @Test
    public void addVertexTest() {
        graph.createIndex("age", Vertex.class);

        graph.createIndex("weight", Edge.class);


        GraphTraversalSource g = graph.traversal(GraphTraversalSource.build().engine(StandardTraversalEngine.build()));

        //System.out.println("g=" + g);


        System.out.println("traversed edge" + g.V().toList());  //.bothE("knows").has("weight", 0.5f).tryNext().orElse(null));


        //g.addV()

        //g.addV().addE()

        Vertex marko = graph.addVertex(T.label, "person", T.id, 1, "name", "marko", "age", 29);
        Vertex vadas = graph.addVertex(T.label, "person", T.id, 2, "name", "vadas", "age", 27);
        Vertex lop = graph.addVertex(T.label, "software", T.id, 3, "name", "lop", "lang", "java");
        Vertex josh = graph.addVertex(T.label, "person", T.id, 4, "name", "josh", "age", 32);
        Vertex ripple = graph.addVertex(T.label, "software", T.id, 5, "name", "ripple", "lang", "java");
        Vertex peter = graph.addVertex(T.label, "person", T.id, 6, "name", "peter", "age", 35);


        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5f, "weight1", 10.6f);
        marko.addEdge("knows", josh, T.id, 8, "weight", 1.0f);
        marko.addEdge("created", lop, T.id, 9, "weight", 0.4f);


        josh.addEdge("created", ripple, T.id, 10, "weight", 1.0f);
        josh.addEdge("created", lop, T.id, 11, "weight", 0.4f);
        peter.addEdge("created", lop, T.id, 12, "weight", 0.2f);


        Iterator<Vertex> iter = graph.vertices(1);
        while (iter.hasNext()) {
            Vertex test = iter.next();
            Iterator<VertexProperty<Object>> properties = test.properties();
            while (properties.hasNext()) {
                System.out.println("vertex:" + test + "\tproperties:" + properties.next());
            }


            Iterator<Edge> edges = test.edges(Direction.BOTH);
            while (edges.hasNext()) {
                Edge edge = edges.next();
                System.out.println("Edge: " + edge);
                Iterator<Property<Object>> edge_properties = edge.properties();
                while (edge_properties.hasNext()) {
                    System.out.println("edge:" + test + "\tproperties:" + edge_properties.next());
                }
            }

            //System.out.println(iter.next().edges(Direction.BOTH).hasNext());
        }
    }


    @Test
    public void PerfTest() {
        graph.createIndex("name", Vertex.class);


        long start = System.currentTimeMillis();
        int ITERATIONS = 10000;

        for (int i = 0; i < ITERATIONS; i++) {
            graph.addVertex(T.label, "person", T.id, 200 + i, "name", "marko" + i, "age", 29);
        }
        long end = System.currentTimeMillis() - start;
        System.out.println("write time takes to add " + ITERATIONS + " vertices (ms):\t" + end);

        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            graph.vertices(200 + i).next().property("name");
        }
        end = System.currentTimeMillis() - start;
        System.out.println("read time takes to read " + ITERATIONS + " vertices (ms):\t" + end);

        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            graph.vertices(200).next().property("name");
        }
        end = System.currentTimeMillis() - start;
        System.out.println("read time takes to access same vertex " + ITERATIONS + " times (ms):\t" + end);


        Vertex supernode = graph.vertices(200).next();
        Vertex supernodeSink = graph.vertices(201).next();


        start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            supernode.addEdge("knows", supernodeSink, T.id, 700 + i, "weight", 0.5f);
        }
        end = System.currentTimeMillis() - start;
        System.out.println("time to add " + ITERATIONS + " edges (ms):\t" + end);

        start = System.currentTimeMillis();
        supernode.edges(Direction.BOTH);
        end = System.currentTimeMillis() - start;
        System.out.println("time to read " + ITERATIONS + " edges (ms):\t" + end);


        start = System.currentTimeMillis();
        Iterator<Edge> test = supernode.edges(Direction.OUT, "knows");
        end = System.currentTimeMillis() - start;

        System.out.println("time to read " + ITERATIONS + " cached edges (ms):\t" + end);
        long count = IteratorUtils.count(test);
        System.out.println("got edges: " + count);


    }

    @Test
    public void IndexTest() {

        graph.createIndex("age", Vertex.class);
        System.out.println("started writing...");
        int i = 0;
        long startw = System.currentTimeMillis();

        while (i < 500) {
            graph.addVertex(T.label, "person", T.id, "index" + i, "name", "marko", "age", 29);
            i++;
        }

        while (i < 500) {
            graph.addVertex(T.label, "personal", T.id, "index" + (5000 + i), "name", "marko", "age", 29);
            i++;
        }

        while (i < 200000) {
            graph.addVertex(T.label, "movie", T.id, "index" + (10000 + i), "name", "marko");
            i++;
        }
        graph.addVertex(T.label, "personal", T.id, ++i, "name", "marko", "age", 30);

        graph.addVertex(T.label, "personal", T.id, ++i, "name", "marko", "age", 31);
        graph.addVertex(T.label, "personal", T.id, ++i, "name", "marko", "age", 31);


        long endw = System.currentTimeMillis();
        System.out.println("time taken to write:" + (endw - startw));


        GraphTraversalSource g = graph.traversal(GraphTraversalSource.build().engine(StandardTraversalEngine.build()));
        System.out.println("starting search....");
        long start = System.currentTimeMillis();
        System.out.println(g.V().has("age", 31).toList());
        long end = System.currentTimeMillis();
        System.out.println("time taken to search:" + (end - start));
    }


    @Test
    public void edgeIndexTest() {


        Vertex v = graph.addVertex(T.label, "personal", T.id, "indextest" + 1, "name", "marko", "age", 30);
        Vertex outV = graph.addVertex(T.label, "personal", T.id, "indextest" + 2, "name", "polo", "age", 30);

        //graph.createIndex("directed", Edge.class);

        IntStream.range(0, 1000000).forEach(value -> {
            v.addEdge("movie", outV, T.id, value, "directed", "test1" + (value % 10000));
        });


        GraphTraversalSource g = graph.traversal(GraphTraversalSource.build().engine(StandardTraversalEngine.build()));

        long start;
        String value = "test1" + 10;
        //for (int i = 0; i < 10; i++) {
        start = System.currentTimeMillis();
        g.E().has("directed", value).toList();
        System.out.println("time taken to search:" + (System.currentTimeMillis() - start));
        //}
    }


    @Test
    public void edgeLabelsTest() {

        Vertex v = graph.addVertex(T.label, "personal", T.id, "edgeTest" + 1, "name", "marko", "age", 30);
        Vertex outV = graph.addVertex(T.label, "personal", T.id, "edgeTest" + 2, "name", "polo", "age", 30);

        int i = 0;
        while (i < 2) {
            v.addEdge("person", outV, T.id, "index" + i);
            i++;
        }

        while (i < 10000) {
            v.addEdge("person1", outV, T.id, "index" + i);
            i++;
        }

        while (i < 200000) {
            v.addEdge("person2", outV, T.id, "index" + i);
            i++;
        }

        GraphTraversalSource g = graph.traversal(GraphTraversalSource.build().engine(StandardTraversalEngine.build()));
        long start = System.currentTimeMillis();
        g.V("edgeTest" + 1).outE("person").toList();
        long end = System.currentTimeMillis() - start;
        System.out.println("time taken:" + end);


    }


    @After
    public void close() throws Exception {
        this.graph.close();
    }
}
