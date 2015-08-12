package com.tinkerrocks.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

import java.util.Iterator;


/**
 * Created by ashishn on 8/11/15.
 */
public class RocksTest {
    @Test
    public void addVertexTest() {
        Configuration configuration = new BaseConfiguration();
        RocksGraph rocksGraph = new RocksGraph(configuration);
        RocksVertex marko = (RocksVertex) rocksGraph.addVertex(T.label, "person", T.id, 1, "name", "marko", "age", 29);

        RocksVertex vadas = (RocksVertex) rocksGraph.addVertex(T.label, "person", T.id, 2, "name", "vadas", "age", 27);

        vadas.property("first", "test2");
        Iterator<VertexProperty<Object>> value = vadas.properties();
        while (value.hasNext()) {
            System.out.println("property" + value.next());
        }


        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5f);

        rocksGraph.vertices(2).forEachRemaining(vertex -> System.out.printf("vertex:%s%n", vertex.toString()));
    }
}
