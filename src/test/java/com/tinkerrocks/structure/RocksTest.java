package com.tinkerrocks.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.junit.Test;

import java.util.Iterator;


/**
 * Created by ashishn on 8/11/15.
 */
public class RocksTest {
    @Test
    public void addVertexTest() {
        Configuration configuration = new BaseConfiguration();
        RocksGraph graph = new RocksGraph(configuration);

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

        Iterator<Vertex> iter = graph.vertices();
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
}
