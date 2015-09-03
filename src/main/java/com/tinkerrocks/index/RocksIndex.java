package com.tinkerrocks.index;

import com.tinkerrocks.structure.RocksEdge;
import com.tinkerrocks.structure.RocksGraph;
import com.tinkerrocks.structure.RocksVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

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


    public void put(final String key, final Object value, final T element) {
        try {
            this.rocksGraph.getStorageHandler().getIndexDB().putIndex(indexClass, key, value, (byte[]) element.id());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    @SuppressWarnings("unchecked")
    public List<T> get(final String key, final Object value) {
        List<T> results = null;
        try {
            List<byte[]> ids = this.rocksGraph.getStorageHandler().getIndexDB().getIndex(indexClass, key, value);
            results = new ArrayList<>();
            if (indexClass.isAssignableFrom(RocksVertex.class)) {
                for (byte[] id : ids) {
                    results.add((T) new RocksVertex(id, rocksGraph));
                }
            }
            if (indexClass.isAssignableFrom(RocksEdge.class)) {
                for (byte[] id : ids) {
                    results.add((T) new RocksEdge(id, rocksGraph));
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return results;
    }

    public long count(final String key, final Object value) {
        return 0;
    }

    public void removeElement(final T element) {


    }


    @SuppressWarnings("unchecked")
    public void createKeyIndex(final String key) {

        Iterator<? extends Element> iterator = null;
        boolean hasKey;

        if (Vertex.class.isAssignableFrom(this.indexClass)) {
            hasKey = this.rocksGraph.getVertexIndex().getIndexedKeys().contains(key);
            if (!hasKey) {
                //todo: move to stream and try parallel stream
                iterator = this.rocksGraph.vertices();
            }
            this.rocksGraph.getStorageHandler().getIndexDB().createIndex(Vertex.class, key);
        } else {
            hasKey = this.rocksGraph.getEdgeIndex().getIndexedKeys().contains(key);
            if (!hasKey) {
                //todo: move to stream and try parallel stream
                iterator = this.rocksGraph.edges();
            }
            this.rocksGraph.getStorageHandler().getIndexDB().createIndex(Edge.class, key);
        }

        if (hasKey || iterator == null) {
            return;
        }

        iterator.forEachRemaining(element -> {
            if (element.property(key).isPresent()) {
                this.put(key, element.property(key).value(), (T) element);
            }
        });

    }


    public void dropKeyIndex(final String key) throws Exception {
        this.rocksGraph.getStorageHandler().getIndexDB().dropIndex(indexClass, key);
    }

    public Set<String> getIndexedKeys() {
        return this.rocksGraph.getStorageHandler().getIndexDB().getIndexedKeys(this.indexClass);
    }

    public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
        if (this.getIndexedKeys().contains(key)) {
            if (oldValue != null)
                this.remove(key, oldValue, element);
            this.put(key, newValue, element);
        }
    }


    public void autoRemove(final String key, final Object oldValue, final T element) {
        if (this.getIndexedKeys().contains(key))
            this.remove(key, oldValue, element);
    }

    public void remove(final String key, final Object value, final T element) {
        this.rocksGraph.getStorageHandler().getIndexDB().removeIndex(this.indexClass, key, value, (byte[]) element.id());
    }

    public static List<Vertex> queryVertexIndex(final RocksGraph graph, final String key, final Object value) {
        return null == graph.getVertexIndex() ? Collections.emptyList() : graph.getVertexIndex().get(key, value);
    }

    public static List<Edge> queryEdgeIndex(final RocksGraph graph, final String key, final Object value) {
        return null == graph.getEdgeIndex() ? Collections.emptyList() : graph.getEdgeIndex().get(key, value);
    }


}
