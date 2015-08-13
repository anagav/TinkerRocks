package com.tinkerrocks.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;


/**
 * <p>
 * Test class
 * </p>
 * Created by ashishn on 8/11/15.
 */
public class RocksTest {
    RocksGraph graph;

    @Before
    public void setup() {
        Configuration configuration = new BaseConfiguration();
        graph = new RocksGraph(configuration);


    }


    @Test
    public void addVertexTest() {

        Vertex marko = graph.addVertex(T.label, "person", T.id, 1, "name", "marko", "age", 29);
        Vertex vadas = graph.addVertex(T.label, "person", T.id, 2, "name", "vadas", "age", 27);
        Vertex lop = graph.addVertex(T.label, "software", T.id, 3, "name", "lop", "lang", "java");
        Vertex josh = graph.addVertex(T.label, "person", T.id, 4, "name", "josh", "age", 32);
        Vertex ripple = graph.addVertex(T.label, "software", T.id, 5, "name", "ripple", "lang", "java");
        Vertex peter = graph.addVertex(T.label, "person", T.id, 6, "name", "peter", "age", 35);


        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5f);
        marko.addEdge("knows", josh, T.id, 8, "weight", 1.0f);
        marko.addEdge("created", lop, T.id, 9, "weight", 0.4f);


        josh.addEdge("created", ripple, T.id, 10, "weight", 1.0f);
        josh.addEdge("created", lop, T.id, 11, "weight", 0.4f);
        peter.addEdge("created", lop, T.id, 12, "weight", 0.2f);

        Iterator<Vertex> iter = graph.vertices(4);
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
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            graph.addVertex(T.label, "person", T.id, 200 + i, "name", "marko" + i, "age", 29);
        }
        long end = System.currentTimeMillis() - start;
        System.out.println("write time takes to add 1000000 vertices (ms):\t" + end);

        start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            graph.vertices(200 + i).next().property("name");
        }
        end = System.currentTimeMillis() - start;
        System.out.println("read time takes to read 1000000 vertices (ms):\t" + end);

        start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            graph.vertices(200).next().property("name");
        }
        end = System.currentTimeMillis() - start;
        System.out.println("read time takes to access same vertex 1000000 times (ms):\t" + end);


        Vertex supernode = graph.vertices(200).next();
        Vertex supernodeSink = graph.vertices(201).next();


        start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            supernode.addEdge("knows", supernodeSink, T.id, 700 + i, "weight", 0.5f);
        }
        end = System.currentTimeMillis() - start;
        System.out.println("time to add 1000000 edges (ms):\t" + end);

        start = System.currentTimeMillis();
        supernode.edges(Direction.BOTH);
        end = System.currentTimeMillis() - start;
        System.out.println("time to read 1000000 edges (ms):\t" + end);


        start = System.currentTimeMillis();
        long count = IteratorUtils.count(supernode.edges(Direction.OUT, "knows"));
        System.out.println("got edges: " + count);
        end = System.currentTimeMillis() - start;
        System.out.println("time to read 1000000 cached edges (ms):\t" + end);


    }

    @After
    public void close() throws Exception {
        this.graph.close();
    }
}
