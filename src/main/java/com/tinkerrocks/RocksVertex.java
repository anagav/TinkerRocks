package com.tinkerrocks;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import storage.StorageHandler;

import java.util.Iterator;

/**
 * Created by ashishn on 8/5/15.
 */
public class RocksVertex extends RocksElement implements Vertex {


    public RocksVertex(String id, String label, StorageHandler storageHandler, RocksGraph rocksGraph) {
        super(id, label, storageHandler, rocksGraph);
    }

    /**
     * Add an outgoing edge to the vertex with provided label and edge properties as key/value pairs.
     * These key/values must be provided in an even number where the odd numbered arguments are {@link String}
     * property keys and the even numbered arguments are the related property values.
     *
     * @param label     The label of the edge
     * @param inVertex  The vertex to receive an incoming edge from the current vertex
     * @param keyValues The key/value pairs to turn into edge properties
     * @return the newly created edge
     */
    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id());
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
//        if (ElementHelper.getIdValue(keyValues).isPresent())
//            throw Edge.Exceptions.userSuppliedIdsNotSupported();


        return storageHandler.getVertexDB().addEdge(id, label, this, inVertex, keyValues);
    }

    /**
     * Set the provided key to the provided value using default {@link VertexProperty.Cardinality} for that key.
     * The default cardinality can be vendor defined and is usually tied to the graph schema.
     * If the vendor does not have a preference, then the default cardinality should be {@link VertexProperty.Cardinality#single}.
     * The provided key/values are the properties of the newly created {@link VertexProperty}.
     * These key/values must be provided in an even number where the odd numbered arguments are {@link String}.
     *
     * @param key       the key of the vertex property
     * @param value     The value of the vertex property
     * @param keyValues the key/value pairs to turn into vertex property properties
     * @return the newly created vertex property
     */
    @Override
    public <V> VertexProperty<V> property(String key, V value, Object... keyValues) {
        return null;
    }

    /**
     * Create a new vertex property.
     * If the cardinality is {@link VertexProperty.Cardinality#single}, then set the key to the value.
     * If the cardinality is {@link VertexProperty.Cardinality#list}, then add a new value to the key.
     * If the cardinality is {@link VertexProperty.Cardinality#set}, then only add a new value if that value doesn't already exist for the key.
     * If the value already exists for the key, add the provided key value vertex property properties to it.
     *
     * @param cardinality the desired cardinality of the property key
     * @param key         the key of the vertex property
     * @param value       The value of the vertex property
     * @param keyValues   the key/value pairs to turn into vertex property properties
     * @return the newly created vertex property
     */
    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        return null;
    }

    /**
     * Gets an {@link Iterator} of incident edges.
     *
     * @param direction  The incident direction of the edges to retrieve off this vertex
     * @param edgeLabels The labels of the edges to retrieve. If no labels are provided, then get all edges.
     * @return An iterator of edges meeting the provided specification
     */
    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return null;
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
        return null;
    }

    /**
     * Get the graph that this element is within.
     *
     * @return the graph of this element
     */
    @Override
    public Graph graph() {
        return null;
    }

    /**
     * Removes the {@code Element} from the graph.
     */
    @Override
    public void remove() {

    }

    /**
     * Get an {@link Iterator} of properties.
     *
     * @param propertyKeys
     */
    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        return null;
    }
}
