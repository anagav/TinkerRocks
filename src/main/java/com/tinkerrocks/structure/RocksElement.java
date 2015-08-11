package com.tinkerrocks.structure; /**
 * Created by ashishn on 8/4/15.
 */

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.RocksDBException;

import java.util.*;
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
            this.rocksGraph.getStorageHandler().getVertexDB().setProperty((byte[]) this.id(), key, value);
        } else {
            this.rocksGraph.getStorageHandler().getEdgeDB().setProperty((String) this.id(), key, value);
        }
        return new RocksProperty<>(this, key, value);
    }


    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
    }

    /**
     * Get an {@link Iterator} of properties.
     *
     * @param propertyKeys
     */
    @Override
    public <V> Iterator<? extends Property<V>> properties(String... propertyKeys) {
        Map<String, byte[]> properties = new HashMap<>(30);
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
        propertiesList.addAll(properties.entrySet().stream()
                .map(property -> new RocksProperty<>(this,
                        property.getKey(), (V) property.getValue())).collect(Collectors.toList()));

        return propertiesList.iterator();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

}
