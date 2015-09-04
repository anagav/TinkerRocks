package com.tinkerrocks.structure; /**
 * Created by ashishn on 8/4/15.
 */

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class RocksElement implements Element {

    protected final byte[] id;
    protected final String label;


    protected final RocksGraph rocksGraph;
    protected boolean removed = false;

    protected RocksElement(final byte[] id, final String label, RocksGraph rocksGraph) {
        this.id = id;
        this.label = label;
        this.rocksGraph = rocksGraph;
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public Object id() {
        return this.id;
    }


    @Override
    public String label() {
        return this.label;
    }

    /**
     * Add or set a property value for the {@code Element} given its key.
     *
     * @param key
     * @param value
     */
    @Override
    public <V> Property<V> property(String key, V value) {
        ElementHelper.validateProperty(key, value);

        if (this instanceof Vertex) {
            this.rocksGraph.getVertexIndex().autoUpdate(key, value, property(key).value(), (Vertex) this);
            this.rocksGraph.vertices(this.id()).next().property(key, value);
        } else {
            this.rocksGraph.getEdgeIndex().autoUpdate(key, value, property(key).value(), (Edge) this);
            this.rocksGraph.getStorageHandler().getEdgeDB().setProperty((byte[]) this.id(), key, value);
        }
        return new RocksProperty<>(this, key, value);
    }


    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id());
    }

    /**
     * Get an {@link Iterator} of properties.
     *
     * @param propertyKeys
     */
    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<? extends Property<V>> properties(String... propertyKeys) {
        Map<String, Object> properties = new HashMap<>(32);

        try {
            if (this instanceof Vertex) {
                return ((RocksVertex) this).properties(propertyKeys);
            } else {
                properties = this.rocksGraph.getStorageHandler().getEdgeDB().getProperties(this, propertyKeys);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        List<Property<V>> propertiesList = properties.entrySet().stream().map(property -> new RocksProperty<>(this,
                property.getKey(), (V) property.getValue())).collect(Collectors.toList());

        return propertiesList.iterator();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

}
