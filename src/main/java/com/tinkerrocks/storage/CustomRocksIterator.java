package com.tinkerrocks.storage;

import com.tinkerrocks.structure.RocksEdge;
import com.tinkerrocks.structure.RocksElement;
import com.tinkerrocks.structure.RocksGraph;
import com.tinkerrocks.structure.RocksVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by ashishn on 10/23/15.
 */
public class CustomRocksIterator<T extends Element> implements Iterator<T> {
    private final static Logger LOGGER = LoggerFactory.getLogger(CustomRocksIterator.class);
    private RocksIterator iterator;
    private RocksGraph rocksGraph;
    private Class<T> elementClass;
    private boolean isFirst = true;


    /**
     * @param iterator
     * @param rocksGraph
     * @param elementClass
     */
    public CustomRocksIterator(RocksIterator iterator, RocksGraph rocksGraph, Class<T> elementClass) {
        this.iterator = iterator;
        this.rocksGraph = rocksGraph;
        this.elementClass = elementClass;
    }


    /**
     * @return
     */
    @Override
    public boolean hasNext() {
        if (isFirst) {
            iterator.seekToFirst();
            isFirst = false;
        }
        boolean isValid = iterator.isValid();
        if (!isValid) {
            this.close();
        }
        return isValid;
    }

    private void close() {
        iterator.dispose();
    }

    @Override
    public T next() {
        if (!iterator.isValid()) {
            this.close();
            throw new NoSuchElementException();
        }
        RocksElement element = null;
        if (Vertex.class.isAssignableFrom(elementClass)) {
            element = new RocksVertex(iterator.key(), new String(iterator.value()), rocksGraph);
        }
        if (Edge.class.isAssignableFrom(elementClass)) {
            element = new RocksEdge(iterator.key(), new String(iterator.value()), rocksGraph);
        }
        iterator.next();
        return elementClass.cast(element);
    }


}
