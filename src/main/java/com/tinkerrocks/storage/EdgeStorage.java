package com.tinkerrocks.storage;

import com.tinkerrocks.structure.RocksEdge;
import com.tinkerrocks.structure.RocksElement;
import com.tinkerrocks.structure.RocksGraph;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.List;
import java.util.Map;

/**
 * Created by ashishn on 8/28/15.
 */
public interface EdgeStorage extends CommonStorage {
    public <V> void setProperty(byte[] id, String key, V value);

    void addEdge(byte[] edge_id, String label, RocksElement inVertex, RocksElement outVertex, Object[] keyValues)
            throws Exception;

    List<byte[]> getVertexIDs(byte[] edgeId, Direction direction);

    Map<String, Object> getProperties(RocksElement element, String[] propertyKeys) throws Exception;

    List<Edge> edges(List<byte[]> ids, RocksGraph rocksGraph) throws Exception;

    RocksEdge getEdge(byte[] id, RocksGraph rocksGraph) throws Exception;

    void remove(RocksEdge rocksEdge) throws Exception;

    String getLabel(byte[] id) throws Exception;
}
