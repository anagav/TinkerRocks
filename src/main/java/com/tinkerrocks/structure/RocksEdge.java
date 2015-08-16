package com.tinkerrocks.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.rocksdb.RocksDBException;

import java.util.*;

/**
 * Created by ashishn on 8/5/15.
 */
public class RocksEdge extends RocksElement implements Edge {


    protected Vertex inVertex;
    protected Vertex outVertex;


    public RocksEdge(byte[] id, String label, RocksGraph rocksGraph,
                     Vertex inVertex, Vertex outVertex) {
        super(id, label, rocksGraph);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }

    public RocksEdge(byte[] id, RocksGraph rocksGraph) throws RocksDBException {
        super(id, rocksGraph.getStorageHandler().getEdgeDB().getLabel(id), rocksGraph);
        byte[] inVertexId = rocksGraph.getStorageHandler().getEdgeDB().getVertexIDs(id, Direction.IN).get(0);
        byte[] outVertexId = rocksGraph.getStorageHandler().getEdgeDB().getVertexIDs(id, Direction.OUT).get(0);
        this.inVertex = rocksGraph.getStorageHandler().getVertexDB().getVertex(inVertexId, rocksGraph);
        this.outVertex = rocksGraph.getStorageHandler().getVertexDB().getVertex(outVertexId, rocksGraph);
    }


    /**
     * Retrieve the vertex (or vertices) associated with this edge as defined by the direction.
     * If the direction is {@link Direction#BOTH} then the iterator order is: {@link Direction#OUT} then {@link Direction#IN}.
     *
     * @param direction Get the incoming vertex, outgoing vertex, or both vertices
     * @return An iterator with 1 or 2 vertices
     */
    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        checkRemoved();
        ArrayList<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add(outVertex);
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add(inVertex);
        return vertices.iterator();

    }

    /**
     * Get the graph that this element is within.
     *
     * @return the graph of this element
     */
    @Override
    public Graph graph() {
        return this.rocksGraph;
    }

    /**
     * Add or set a property value for the {@code Element} given its key.
     *
     * @param key
     * @param value
     */
    @Override
    public <V> Property<V> property(String key, V value) {
        return super.property(key, value);
    }

    /**
     * Removes the {@code Element} from the graph.
     */
    @Override
    public void remove() {
        try {
            this.rocksGraph.getStorageHandler().getEdgeDB().remove(this);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get an {@link Iterator} of properties.
     *
     * @param propertyKeys
     */
    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {

        Map<String, Object> properties = new HashMap<>();
        try {
            if (this instanceof Vertex) {
                properties = this.rocksGraph.getStorageHandler().getVertexDB().getProperties(this, propertyKeys);
            } else {
                properties = this.rocksGraph.getStorageHandler().getEdgeDB().getProperties(this, propertyKeys);
            }
        } catch (RocksDBException ex) {
            ex.printStackTrace();
        }

        List<Property<V>> propertiesList = new ArrayList<>(properties.size());


        for (Map.Entry<String, Object> property : properties.entrySet()) {
            if (property.getValue() == null) {
                continue;
            }
            propertiesList.add(new RocksVertexProperty<>(this,
                    property.getKey(),
                    (V) property.getValue()));
        }

        return propertiesList.iterator();
    }

    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Edge.class, this.id());
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
        return String.format("E[%s][%s-%s-->%s]", new String((byte[]) this.id()), new String((byte[])
                this.outVertex().id()), this.label(), new String((byte[]) this.inVertex().id()));

    }
}
