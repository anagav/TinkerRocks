package com.tinkerrocks.structure;

import com.tinkerrocks.storage.StorageConstants;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RocksGraphProvider extends AbstractGraphProvider {
    static RocksGraph rocksGraph = RocksGraph.open();


    @Override
    public Graph openTestGraph(Configuration config) {
        System.out.println("----------  this is called");
        return rocksGraph;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String s, Class<?> aClass, String s1, LoadGraphWith.GraphData graphData) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, RocksGraph.class.getName());
            put(StorageConstants.TEST_DATABASE_PREFIX, "true");
        }};
        //return new HashMap<String, Object>();

    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {

    }

    @Override
    public Set<Class> getImplementations() {

        return new HashSet<Class>() {{
            add(RocksEdge.class);
            add(RocksElement.class);
            //add(RocksGraph.class);
            add(RocksProperty.class);
            add(RocksVertex.class);
            add(RocksVertexProperty.class);
        }};
    }
}