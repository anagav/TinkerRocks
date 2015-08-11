package com.tinkerrocks.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;


/**
 * Created by ashishn on 8/11/15.
 */
public class RocksTest {
    @Test
    public void addVertexTest() {
        Configuration configuration = new BaseConfiguration();
        RocksGraph rocksGraph = new RocksGraph(configuration);
        Vertex marko = rocksGraph.addVertex(T.label, "person", T.id, 1, "name", "marko", "age", 29);
        Vertex vadas = rocksGraph.addVertex(T.label, "person", T.id, 2, "name", "vadas", "age", 27);
        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5f);

        rocksGraph.vertices(1).forEachRemaining(vertex -> System.out.println("vertex:" + vertex.toString()));
    }
}
