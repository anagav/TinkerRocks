package com.tinkerrocks.structure;

import com.tinkerrocks.index.RocksIndex;
import com.tinkerrocks.process.traversal.RocksGraphStepStrategy;
import com.tinkerrocks.storage.StorageConstants;
import com.tinkerrocks.storage.StorageHandler;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.RocksDBException;

import java.io.Closeable;
import java.util.*;

/**
 * Created by ashishn on 8/4/15.
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)


public final class RocksGraph implements Graph {


    static {
        TraversalStrategies.GlobalCache.registerStrategies(RocksGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(RocksGraphStepStrategy.instance()));
    }


    public Configuration getConfiguration() {
        return configuration;
    }

    private final Configuration configuration;
    private final StorageHandler storageHandler;

    private RocksIndex<Vertex> vertexIndex = null;
    private RocksIndex<Edge> edgeIndex = null;

    public static RocksGraph open() {
        Configuration configuration = new BaseConfiguration();
        configuration.addProperty(StorageConstants.STORAGE_DIR_PROPERTY, StorageConstants.TEST_DATABASE_PREFIX);
        return new RocksGraph(configuration);
    }


    public static RocksGraph open(Configuration configuration) throws InstantiationException {
        if (configuration == null) {
            throw new InstantiationException("cannot pass null configuration");
        }
        return new RocksGraph(configuration);
    }

    public RocksGraph(Configuration configuration) {
        configuration.setProperty(Graph.GRAPH, RocksGraph.class.getName());
        this.configuration = configuration;
        if (!this.configuration.containsKey(StorageConstants.STORAGE_DIR_PROPERTY)) {
            this.configuration.addProperty(StorageConstants.STORAGE_DIR_PROPERTY, StorageConstants.TEST_DATABASE_PREFIX);
        }
        try {
            this.storageHandler = new StorageHandler(this);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw Exceptions.idArgsMustBeEitherIdOrElement();
        }
        this.vertexIndex = new RocksIndex<>(this, Vertex.class);
        this.edgeIndex = new RocksIndex<>(this, Edge.class);
    }

    public StorageHandler getStorageHandler() {
        return storageHandler;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object id = String.valueOf(ElementHelper.getIdValue(keyValues).orElse(null));
        if (id == null) {
            throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        }
        byte[] idValue = String.valueOf(id).getBytes();
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        RocksVertex vertex = new RocksVertex(idValue, label, this);

        try {
            storageHandler.getVertexDB().addVertex(idValue, label, null);
            Map<String, Object> properties = ElementHelper.asMap(keyValues);

            for (Map.Entry<String, Object> property : properties.entrySet()) {
                vertexIndex.autoUpdate(property.getKey(), property.getValue(), null, vertex);
                vertex.property(property.getKey(), property.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return vertex;

    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> aClass) throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }


    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null == this.vertexIndex) this.vertexIndex = new RocksIndex<>(this, Vertex.class);
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null == this.edgeIndex) this.edgeIndex = new RocksIndex<>(this, Edge.class);
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }


    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        if (vertexIds == null || vertexIds.length == 0) {
            try {
                return storageHandler.getVertexDB().vertices(null, this).iterator();
            } catch (Exception e) {
                e.printStackTrace();
                throw Exceptions.elementNotFound(Vertex.class, "all ids");
            }
        }

        if (vertexIds.length > 1 && !vertexIds[0].getClass().equals(vertexIds[1].getClass()))
            throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();

        List<byte[]> ids = new ArrayList<>(vertexIds.length);
        for (Object vertexId : vertexIds) {
            if (vertexId instanceof RocksElement) {
                ids.add((byte[]) ((RocksElement) vertexId).id());
            } else {
                ids.add(String.valueOf(vertexId).getBytes());
            }
        }

        if (ids.size() == 0) {
            throw Exceptions.elementNotFound(Vertex.class, ids.get(0));
        }

        try {
            return storageHandler.getVertexDB().vertices(ids, this).iterator();
        } catch (Exception e) {
            e.printStackTrace();
            throw Exceptions.elementNotFound(Vertex.class, ids.get(0));
        }
    }


    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return null == this.vertexIndex ? Collections.emptySet() : this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return null == this.edgeIndex ? Collections.emptySet() : this.edgeIndex.getIndexedKeys();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }


    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds.length > 1 && !edgeIds[0].getClass().equals(edgeIds[1].getClass()))
            throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();

        List<byte[]> ids = new ArrayList<>();
        for (Object vertexId : edgeIds) {
            ids.add(String.valueOf(vertexId).getBytes());
        }
        try {
            return storageHandler.getEdgeDB().edges(ids, this).iterator();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //todo place holder
        return new ArrayList<Edge>().iterator();
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        throw Exceptions.variablesNotSupported();
    }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     * <p/>
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     * <p/>
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     * <p/>
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p/>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p/>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     * <p/>
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p/>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {
        storageHandler.close();
    }


    public RocksIndex<Vertex> getVertexIndex() {
        return vertexIndex;
    }


    public RocksIndex<Edge> getEdgeIndex() {
        return edgeIndex;
    }

    /**
     * Gets the {@link Features} exposed by the underlying {@code Graph} implementation.
     */
    @Override
    public Features features() {
        return new Features() {
            @Override
            public GraphFeatures graph() {
                return new GraphFeatures() {
                    @Override
                    public boolean supportsComputer() {
                        return false;
                    }

                    @Override
                    public boolean supportsTransactions() {
                        return false;
                    }

                    @Override
                    public boolean supportsThreadedTransactions() {
                        return false;
                    }

                    @Override
                    public boolean supportsPersistence() {
                        return true;
                    }
                };
            }
        };
    }
}
