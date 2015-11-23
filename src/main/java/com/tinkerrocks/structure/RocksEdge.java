package com.tinkerrocks.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by ashishn on 8/5/15.
 */
public class RocksEdge extends RocksElement implements Edge {
    private final transient static Logger LOGGER = LoggerFactory.getLogger(RocksEdge.class);

    public RocksEdge(byte[] id, String label, RocksGraph rocksGraph) {
        super(id, label, rocksGraph);
    }


    public RocksEdge(byte[] id, String label, RocksGraph rocksGraph,
                     byte[] inVertex, byte[] outVertex, Object[] keyValues) {
        super(id, label, rocksGraph);
        try {
            this.rocksGraph.getStorageHandler().getEdgeDB().addEdge(id, label, inVertex, outVertex, keyValues);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map<String, Object> properties = ElementHelper.asMap(keyValues);
        for (Map.Entry<String, Object> property : properties.entrySet()) {
            property(property.getKey(), property.getValue());
        }

    }

    public RocksEdge(byte[] id, RocksGraph rocksGraph) throws Exception {
        super(id, rocksGraph.getStorageHandler().getEdgeDB().getLabel(id), rocksGraph);
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
        try {
            ArrayList<Vertex> vertices = new ArrayList<>();
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
                byte[] outVertex = rocksGraph.getStorageHandler().getEdgeDB().getVertexIDs(id, Direction.OUT).get(0);
                vertices.add(this.rocksGraph.getStorageHandler().getVertex(outVertex, rocksGraph));
            }
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
                byte[] inVertex = rocksGraph.getStorageHandler().getEdgeDB().getVertexIDs(id, Direction.IN).get(0);
                vertices.add(this.rocksGraph.getStorageHandler().getVertex(inVertex, rocksGraph));
            }
            return vertices.iterator();
        } catch (Exception ex) {
            LOGGER.error("Exception in edge vertices iterator:", ex);
            ex.printStackTrace();
        }
        return Collections.emptyIterator();

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
        } catch (Exception e) {
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
        return (Iterator) super.properties(propertyKeys);
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
                this.inVertex().id()), this.label(), new String((byte[]) this.outVertex().id()));

    }
}
