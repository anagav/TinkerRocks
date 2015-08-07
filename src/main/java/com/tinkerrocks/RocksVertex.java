package com.tinkerrocks;

import org.apache.tinkerpop.gremlin.structure.*;
import org.rocksdb.RocksDBException;
import storage.StorageHandler;

import java.util.Iterator;
import java.util.List;

/**
 * Created by ashishn on 8/5/15.
 */
public class RocksVertex extends RocksElement implements Vertex {


    public RocksVertex(String id, String label, StorageHandler storageHandler, RocksGraph rocksGraph) {
        super(id, label, storageHandler, rocksGraph);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<byte[]> results;
        try {
            results = storageHandler.getVertexDB().getProperties(this, propertyKeys);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return null;
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        return null;
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value, Object... keyValues) {
        return null;
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return null;
    }

    @Override
    public Graph graph() {
        return null;
    }

    @Override
    public void remove() {

    }
}
