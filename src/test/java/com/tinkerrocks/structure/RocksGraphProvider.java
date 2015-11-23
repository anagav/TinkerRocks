package com.tinkerrocks.structure;

import com.tinkerrocks.storage.StorageConstants;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RocksGraphProvider extends AbstractGraphProvider {
    HashMap<String, RocksGraph> paths = new HashMap<>();


    @Override
    public Graph openTestGraph(Configuration config) {
        String path = StorageConstants.TEST_DATABASE_PREFIX + "/" + UUID.randomUUID().toString();
        try {
            FileUtils.forceMkdir(new File(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Configuration configuration = new BaseConfiguration();
        configuration.addProperty(StorageConstants.STORAGE_DIR_PROPERTY, path);

        try {
            RocksGraph graph = RocksGraph.open(configuration);
            paths.put(path, graph);
            return graph;

        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String s, Class<?> aClass, String s1, LoadGraphWith.GraphData graphData) {

        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, RocksGraph.class.getName());
        }};
        //return new HashMap<String, Object>();
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        for (Map.Entry<String, RocksGraph> path : paths.entrySet()) {
            //System.out.println("cleaning up dir:" + path);
            FileUtils.deleteDirectory(new File(path.getKey()));
            path.getValue().close();
        }
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