package com.tinkerrocks.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.RocksDBException;

import java.util.*;

/**
 * Created by ashishn on 8/5/15.
 */
public class RocksVertex extends RocksElement implements Vertex {


    public RocksVertex(byte[] id, String label, RocksGraph rocksGraph) {
        super(id, label, rocksGraph);
    }

    public RocksVertex(byte[] id, RocksGraph rocksGraph) throws RocksDBException {
        super(id, rocksGraph.getStorageHandler().getVertexDB().getLabel(id), rocksGraph);
    }


    /**
     * @param propertyKeys
     * @param <V>
     * @return
     */
    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<byte[]> propKeys = new ArrayList<>();
        for (String propertyKey : propertyKeys) {
            propKeys.add(propertyKey.getBytes());
        }
        try {
            return this.rocksGraph.getStorageHandler().getVertexDB().<V>getProperties(this, propKeys).iterator();
        } catch (Exception e) {
            e.printStackTrace();
            return (new ArrayList<VertexProperty<V>>()).iterator();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
        return this.property(VertexProperty.Cardinality.single, key, value);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");

        ElementHelper.legalPropertyKeyValueArray(keyValues);
        checkRemoved();
        if (label == null) {
            throw Element.Exceptions.labelCanNotBeNull();
        }

        if (label.isEmpty()) {
            throw Element.Exceptions.labelCanNotBeEmpty();
        }


        byte[] edge_id = String.valueOf(ElementHelper
                .getIdValue(keyValues).orElse(UUID.randomUUID().toString().getBytes())).getBytes();  //UUID.randomUUID().toString().getBytes();

        Edge edge = new RocksEdge(edge_id, label, this.rocksGraph, this, inVertex, keyValues);


        try {
            //this.rocksGraph.getStorageHandler().getEdgeDB().addEdge(edge_id, label, (RocksElement) inVertex, this, keyValues);
            this.rocksGraph.getStorageHandler().getVertexDB().addEdge((byte[]) this.id(), edge, inVertex);
        } catch (RocksDBException e) {
            e.printStackTrace();
            return null;
        }
        return edge;
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        return this.property(key, value);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);

        this.rocksGraph.getStorageHandler().getVertexDB().setProperty((byte[]) this.id(), key, value, cardinality);
        properties(key).forEachRemaining(objectVertexProperty -> this.rocksGraph.getVertexIndex().
                autoUpdate(key, value, objectVertexProperty.value(), this));
        return new RocksVertexProperty<>(this, key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);

        HashSet<String> edgeLabelsBytes = new HashSet<>(edgeLabels.length);

        Collections.addAll(edgeLabelsBytes, edgeLabels);

        List<byte[]> edgeIds = this.rocksGraph.getStorageHandler().getVertexDB().getEdgeIDs((byte[]) this.id(),
                direction, edgeLabelsBytes);

        if (edgeIds.size() == 0) {
            return new ArrayList<Edge>().iterator();
        }

        try {
            List<Edge> edges = this.rocksGraph.getStorageHandler().getEdgeDB().edges(edgeIds, this.rocksGraph);
            return edges.iterator();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<Edge>().iterator();
    }

    /**
     * Gets an {@link Iterator} of adjacent vertices.
     *
     * @param direction  The adjacency direction of the vertices to retrieve off this vertex
     * @param edgeLabels The labels of the edges associated with the vertices to retrieve. If no labels are provided, then get all edges.
     * @return An iterator of vertices meeting the provided specification
     */
    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        HashSet<String> edgeLabelsBytes = new HashSet<>(edgeLabels.length);

        Collections.addAll(edgeLabelsBytes, edgeLabels);

        List<byte[]> edgeIds = this.rocksGraph.getStorageHandler().getVertexDB().getEdgeIDs((byte[]) this.id(), direction, edgeLabelsBytes);
        if (edgeIds.size() == 0) {
            return new ArrayList<Vertex>().iterator();
        }
        List<byte[]> vertexIds = new ArrayList<>();


        for (byte[] edgeId : edgeIds) {
            vertexIds.addAll(this.rocksGraph.getStorageHandler().getEdgeDB().getVertexIDs(edgeId, direction));
        }
        try {
            return this.rocksGraph.getStorageHandler().getVertexDB().vertices(vertexIds, this.rocksGraph).iterator();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return new ArrayList<Vertex>().iterator();
    }

    @Override
    public Graph graph() {
        return this.rocksGraph;
    }

    @Override
    public void remove() {
        this.removed = true;
        edges(Direction.BOTH).forEachRemaining(Element::remove);
        try {
            this.rocksGraph.getStorageHandler().getVertexDB().remove(this);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    @Override
    public String toString() {
        return "V" + "[" + new String((byte[]) id()) + "]";
    }
}
