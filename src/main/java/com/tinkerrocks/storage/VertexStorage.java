package com.tinkerrocks.storage;

import com.tinkerrocks.structure.RocksElement;
import com.tinkerrocks.structure.RocksGraph;
import com.tinkerrocks.structure.RocksVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.rocksdb.RocksDBException;

import java.util.Iterator;
import java.util.List;

/**
 * Created by ashishn on 8/28/15.
 */
public interface VertexStorage extends CommonStorage {
    <V> void setProperty(byte[] id, String key, V value, VertexProperty.Cardinality cardinality);

    void addEdge(byte[] vertexId, Edge edge, Vertex inVertex) throws Exception;

    <V> List<VertexProperty<V>> getProperties(RocksElement rocksVertex, String[] propertyKeys) throws Exception;

    List<byte[]> getEdgeIDs(byte[] id, Direction direction, String[] edgeLabels);

    RocksVertex vertex(byte[] id, RocksGraph rocksGraph) throws Exception;

    void remove(RocksVertex rocksVertex) throws Exception;

    void addVertex(byte[] idValue, String label, Object[] keyValues) throws Exception;

    List<Vertex> vertices(List<byte[]> vertexIds, RocksGraph rocksGraph) throws Exception;

    RocksVertex getVertex(byte[] vertexId, RocksGraph rocksGraph) throws RocksDBException;

    String getLabel(byte[] id) throws Exception;

    Iterator<Vertex> getVertices();

    @Override
    void close();

    List<byte[]> getEdgeVertexIDs(byte[] id, Direction direction, String[] edgeLabels);
}
