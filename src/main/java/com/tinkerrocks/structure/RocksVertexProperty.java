package com.tinkerrocks.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by ashishn on 8/5/15.
 */
public class RocksVertexProperty<V> implements VertexProperty<V> {

    protected final Element element;
    protected final String key;
    protected V value;

    public RocksVertexProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;
    }


    /**
     * The key of the property.
     *
     * @return The property key
     */
    @Override
    public String key() {
        return this.key;
    }

    /**
     * The value of the property.
     *
     * @return The property value
     * @throws NoSuchElementException thrown if the property is empty
     */
    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    /**
     * Whether the property is empty or not.
     *
     * @return True if the property exists, else false
     */
    @Override
    public boolean isPresent() {
        return value != null;
    }

    /**
     * Gets the {@link Vertex} that owns this {@code VertexProperty}.
     */
    @Override
    public Vertex element() {
        return (Vertex) this.element;
    }

    /**
     * Remove the property from the associated element.
     */
    @Override
    public void remove() {
        //todo add remove capability
    }

    /**
     * Gets the unique identifier for the graph {@code Element}.
     *
     * @return The byte_id of the element
     */
    @Override
    public Object id() {
        return this.element.id();
    }

    /**
     * Add or set a property value for the {@code Element} given its key.
     *
     * @param key
     * @param value
     */
    @Override
    public <V> Property<V> property(String key, V value) {
        return null;
    }

    /**
     * {@inheritDoc}
     *
     * @param propertyKeys
     */
    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return null;
    }

    @Override
    public String toString() {
        if (!this.isPresent()) return StringFactory.propertyString(VertexProperty.empty());
        final String valueString = String.valueOf(value());
        return "vp" + "[" + key() + "->" + valueString + "]";


        //return StringFactory.propertyString(this);
    }
}
