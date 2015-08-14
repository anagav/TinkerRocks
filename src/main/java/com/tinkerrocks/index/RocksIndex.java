//package com.tinkerrocks.index;
//
//import com.tinkerrocks.structure.RocksGraph;
//import org.apache.tinkerpop.gremlin.structure.Element;
//
//import java.util.List;
//import java.util.Set;
//
///**
// * Created by ashishn on 8/13/15.
// */
//public class RocksIndex<T extends Element> implements index{
//    protected final Class<T> indexClass;
//    RocksGraph rocksGraph;
//
//
//    public RocksIndex(RocksGraph rocksGraph, final Class<T> indexClass) {
//        this.rocksGraph = rocksGraph;
//        this.indexClass = indexClass;
//    }
//
//
//    protected void put(final String key, final Object value, final T element) {
//
//    }
//
//
//    public List<T> get(final String key, final Object value) {
//
//    }
//
//    public long count(final String key, final Object value) {
//
//    }
//
//    public void removeElement(final T element) {
//
//
//    }
//
//
//    public void createKeyIndex(final String key) {
//
//    }
//
//    public void dropKeyIndex(final String key) {
//
//    }
//
//    public Set<String> getIndexedKeys() {
//
//    }
//
//    public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
//
//    }
//
//    public void autoRemove(final String key, final Object oldValue, final T element) {
//
//    }
//
//    public void remove(final String key, final Object value, final T element) {
//
//    }
//
//
//}
