package com.tinkerrocks.index;

import com.tinkerrocks.structure.RocksGraph;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Set;

/**
 * Created by ashishn on 8/13/15.
 */
public class RocksIndex<T extends Element> {
    protected final Class<T> indexClass;
    RocksGraph rocksGraph;


    public RocksIndex(RocksGraph rocksGraph, final Class<T> indexClass) {
        this.rocksGraph = rocksGraph;
        this.indexClass = indexClass;
    }


    protected void put(final String key, final Object value, final T element) {
        if (element instanceof Vertex) {
            this.rocksGraph.getStorageHandler().getIndexDB().putIndex(indexClass, key, value, element.id());
        }
        if (element instanceof Edge) {
            this.rocksGraph.getStorageHandler().getIndexDB().putIndex(indexClass, key, value, element.id());

        }
    }


    public List<T> get(final String key, final Object value) {

        return null;
    }

    public long count(final String key, final Object value) {

        return 0;
    }

    public void removeElement(final T element) {


    }


    public void createKeyIndex(final String key) {

    }

    public void dropKeyIndex(final String key) {

    }

    public Set<String> getIndexedKeys() {

        return null;
    }

    public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {

    }

    public void autoRemove(final String key, final Object oldValue, final T element) {

    }

    public void remove(final String key, final Object value, final T element) {

    }


}
