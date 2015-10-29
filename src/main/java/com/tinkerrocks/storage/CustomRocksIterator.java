package com.tinkerrocks.storage;

import com.tinkerrocks.structure.RocksEdge;
import com.tinkerrocks.structure.RocksElement;
import com.tinkerrocks.structure.RocksGraph;
import com.tinkerrocks.structure.RocksVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.rocksdb.RocksIterator;

import java.util.Iterator;

/**
 * Created by ashishn on 10/23/15.
 */
public class CustomRocksIterator<T extends Element> implements Iterator<T> {
    RocksIterator iterator;
    RocksGraph rocksGraph;
    Class<T> elementClass;
    boolean isFirst = true;


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
            throw new RuntimeException("iterator not valid");
        }
        try {
            RocksElement element = null;
            if (Vertex.class.isAssignableFrom(elementClass)) {
                element = new RocksVertex(iterator.key(), new String(iterator.value()), rocksGraph);
            }
            if (Edge.class.isAssignableFrom(elementClass)) {
                element = new RocksEdge(iterator.key(), new String(iterator.value()), rocksGraph);
            }
            iterator.next();
            return elementClass.cast(element);
        } catch (Exception e) {
            e.printStackTrace();
            this.close();
            throw new RuntimeException(e);
        }
    }


}
