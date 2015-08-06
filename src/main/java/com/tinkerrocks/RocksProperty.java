package com.tinkerrocks;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.NoSuchElementException;

/**
 * Created by ashishn on 8/5/15.
 */
public class RocksProperty<V> implements Property<V> {


    protected final Element element;
    protected final String key;
    protected V value;

    public RocksProperty(final Element element, final String key, final V value) {
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
        return null != this.value;
    }

    /**
     * Get the element that this property is associated with.
     *
     * @return The element associated with this property (i.e. {@link Vertex}, {@link Edge}, or {@link VertexProperty}).
     */
    @Override
    public Element element() {
        return this.element;
    }

    /**
     * Remove the property from the associated element.
     */
    @Override
    public void remove() {


    }
}
