package com.tinkerrocks;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.RocksDBException;
import storage.StorageHandler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by ashishn on 8/5/15.
 */
public class RocksVertex extends RocksElement implements Vertex {


    public RocksVertex(String id, String label, StorageHandler storageHandler, RocksGraph rocksGraph) {
        super(id, label, storageHandler, rocksGraph);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        Map<String, byte[]> results;
        List<VertexProperty<V>> props = new ArrayList<>();
        try {
            results = storageHandler.getVertexDB().getProperties(this, propertyKeys);
        } catch (RocksDBException e) {
            e.printStackTrace();
            return null;
        }
        results.forEach((s, bytes) -> {
            props.add(new RocksVertexProperty<>(this, s, (V) new String(bytes)));
        });

        return props.iterator();

    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        storageHandler.getVertexDB().setProperty(this.id, key, value);
        return new RocksVertexProperty<>(this, key, value);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");

        ElementHelper.legalPropertyKeyValueArray(keyValues);
        checkRemoved();

        Object edge_id = ElementHelper.getIdValue(keyValues);

        try {
            this.storageHandler.getEdgeDB().addEdge(edge_id, label, this, inVertex, keyValues);
        } catch (RocksDBException e) {
            e.printStackTrace();
            return null;
        }
        return new RocksEdge((String) edge_id, label, storageHandler, this.rocksGraph, this, inVertex);
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        try {
            this.storageHandler.getVertexDB().addProperty(ElementHelper.getIdValue(keyValues).toString(), key, (String) value);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return new RocksVertexProperty<>(this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        List<String> edgeIds = this.storageHandler.getVertexDB().getEdgeIDs(direction, edgeLabels);
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
