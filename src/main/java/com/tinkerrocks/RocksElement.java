package com.tinkerrocks; /**
 * Created by ashishn on 8/4/15.
 */

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import storage.StorageHandler;

import java.util.Iterator;

public abstract class RocksElement implements Element {

    protected final String id;
    protected final String label;


    protected final StorageHandler storageHandler;
    protected final RocksGraph rocksGraph;
    protected boolean removed = false;

    protected RocksElement(final String id, final String label, StorageHandler storageHandler, RocksGraph rocksGraph) {
        this.id = id;
        this.label = label;
        this.storageHandler = storageHandler;
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
            storageHandler.getVertexDB().setProperty((String) this.id(), key, value);
        } else {
            storageHandler.getEdgeDB().setProperty((String) this.id(), key, value);
        }
        return new RocksProperty<>(this, key, value);
    }

    /**
     * Get an {@link Iterator} of properties.
     *
     * @param propertyKeys
     */
    @Override
    public <V> Iterator<? extends Property<V>> properties(String... propertyKeys) {
        if (this instanceof Vertex) {
            return storageHandler.getVertexDB().getPropertiesIterator(this, id, propertyKeys);
        } else {
            return storageHandler.getEdgeDB().getPropertiesIterator(this, id, propertyKeys);
        }
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

}
