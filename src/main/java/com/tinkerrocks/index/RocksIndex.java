package com.tinkerrocks.index;

import com.tinkerrocks.structure.RocksGraph;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * Created by ashishn on 8/13/15.
 */
public class RocksIndex<T extends Element> {
    RocksGraph rocksGraph;
    protected final Class<T> indexClass;


    public RocksIndex(RocksGraph rocksGraph, final Class<T> indexClass) {
        this.rocksGraph = rocksGraph;
        this.indexClass = indexClass;
    }

}
