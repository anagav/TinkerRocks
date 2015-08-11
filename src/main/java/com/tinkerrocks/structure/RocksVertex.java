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

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        Map<String, byte[]> results;
        List<VertexProperty<V>> props = new ArrayList<>();
        try {
            results = this.rocksGraph.getStorageHandler().getVertexDB().getProperties(this, propertyKeys);
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
        this.rocksGraph.getStorageHandler().getVertexDB().setProperty(this.id, key, value);
        return new RocksVertexProperty<>(this, key, value);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");

        ElementHelper.legalPropertyKeyValueArray(keyValues);
        checkRemoved();

        byte[] edge_id = String.valueOf(ElementHelper
                .getIdValue(keyValues).orElse(UUID.randomUUID().toString().getBytes())).getBytes();  //UUID.randomUUID().toString().getBytes();


        try {
            this.rocksGraph.getStorageHandler().getEdgeDB().addEdge(edge_id, label, this, inVertex, keyValues);
            System.out.println("adding Edge with id:" + new String(edge_id));
            this.rocksGraph.getStorageHandler().getVertexDB().addEdge(this.id, edge_id, inVertex);
        } catch (RocksDBException e) {
            e.printStackTrace();
            return null;
        }
        return new RocksEdge(edge_id, label, this.rocksGraph, this, inVertex);
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        try {
            this.rocksGraph.getStorageHandler().getVertexDB().addProperty(ElementHelper.getIdValue(keyValues).toString(), key, (String) value);
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
        List<byte[]> edgeIds = this.rocksGraph.getStorageHandler().getVertexDB().getEdgeIDs(this.id, direction, edgeLabels);
        try {
            List<Edge> edges = this.rocksGraph.getStorageHandler().getEdgeDB().edges(edgeIds, this.rocksGraph);
            for (Edge edge : edges) {
                System.out.println("edge: " + edge);
            }

        } catch (RocksDBException e) {
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
        List<byte[]> edgeIds = this.rocksGraph.getStorageHandler().getVertexDB().getEdgeIDs(this.id, direction, edgeLabels);
        List<byte[]> vertexIds = new ArrayList<>(100);
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
        //todo:delete vertex;
    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p/>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return "Vertex: \n id:" + new String(id) + "edges:" + edges(Direction.BOTH, "");
    }
}
