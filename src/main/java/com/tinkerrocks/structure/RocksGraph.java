package com.tinkerrocks.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.RocksDBException;
import storage.StorageHandler;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Created by ashishn on 8/4/15.
 */

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)


public final class RocksGraph implements Graph {


    private final Configuration configuration;
    private final StorageHandler storageHandler;


    public static RocksGraph open(final Configuration configuration) throws InstantiationException {
        return new RocksGraph(configuration);
    }

    public RocksGraph(Configuration configuration) {
        configuration.setProperty(Graph.GRAPH, RocksGraph.class.getName());
        this.configuration = configuration;
        try {
            this.storageHandler = new StorageHandler();
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw Exceptions.idArgsMustBeEitherIdOrElement();
        }
    }

    public StorageHandler getStorageHandler() {
        return storageHandler;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        byte[] idValue = String.valueOf(ElementHelper.getIdValue(keyValues).orElse(UUID.randomUUID().toString().getBytes())).getBytes();  //UUID.randomUUID().toString().getBytes();
        System.out.println("adding vertex with byte_id:" + new String(idValue));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        try {
            storageHandler.getVertexDB().addVertex(idValue, label, keyValues);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return new RocksVertex(idValue, label, this);

    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> aClass) throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        if (vertexIds == null || vertexIds.length == 0) {
            try {
                return storageHandler.getVertexDB().vertices(null, this).iterator();
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        if (vertexIds.length > 1 && !vertexIds[0].getClass().equals(vertexIds[1].getClass()))
            throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();

        List<byte[]> ids = new ArrayList<>(vertexIds.length);
        for (Object vertexId : vertexIds) {
            ids.add(String.valueOf(vertexId).getBytes());
        }
        try {
            return storageHandler.getVertexDB().vertices(ids, this).iterator();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return new ArrayList<Vertex>().iterator();
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
        } catch (RocksDBException e) {
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


    /**
     * Construct a particular {@link Io} implementation for reading and writing the {@code Graph} and other data.
     * End-users will "select" the {@link Io} implementation that they want to use by supplying the {@link Io.Builder}
     * that constructs it.  In this way, {@code Graph} vendors can supply their {@link IoRegistry} to that builder
     * thus allowing for custom serializers to be auto-configured into the {@link Io} instance.  Registering custom
     * serializers is particularly useful for those graphs that have complex types for {@link Element} identifiers.
     * </br>
     * For those graphs that do not need to register any custom serializers, the default implementation should suffice.
     * If the default is overriden, take care to register the current graph to the {@link Io.Builder} via the
     * {@link Io.Builder#graph(Graph)} method.
     *
     * @param builder
     */
    @Override
    public <I extends Io> I io(Io.Builder<I> builder) {
        return null;
    }

    /**
     * Gets the {@link Features} exposed by the underlying {@code Graph} implementation.
     */
    @Override
    public Features features() {
        return null;
    }
}
