package com.tinkerrocks.process.traversal;

import com.tinkerrocks.index.RocksIndex;
import com.tinkerrocks.structure.RocksGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ashishn on 8/15/15.
 */
public class RocksGraphStep<S extends Element> extends GraphStep<S> implements HasContainerHolder {

    public final List<HasContainer> hasContainers = new ArrayList<>();


    public RocksGraphStep(final GraphStep<S> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.getIds());

        originalGraphStep.getLabels().forEach(this::addLabel);
        if ((this.ids.length == 0 || !(this.ids[0] instanceof Element))) {
            this.setIteratorSupplier(() -> Vertex.class.isAssignableFrom(this.returnClass) ?
                    (Iterator) this.vertices() : (Iterator) this.edges());
        }

    }

    private Iterator<? extends Edge> edges() {
        final RocksGraph graph = (RocksGraph) this.getTraversal().getGraph().get();
        final HasContainer indexedContainer = getIndexKey(Edge.class);
        if (this.ids != null && this.ids.length > 0)
            return this.iteratorList(graph.edges(this.ids));
        else
            return null == indexedContainer ?
                    this.iteratorList(graph.edges()) :
                    RocksIndex.queryEdgeIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate().getValue()).stream()
                            .filter(edge -> HasContainer.testAll(edge, this.hasContainers))
                            .collect(Collectors.<Edge>toList()).iterator();
    }

    private Iterator<? extends Vertex> vertices() {
        final RocksGraph graph = (RocksGraph) this.getTraversal().getGraph().get();
        final HasContainer indexedContainer = getIndexKey(Vertex.class);
        if (this.ids != null && this.ids.length > 0)
            return this.iteratorList(graph.vertices(this.ids));
        else {
            return null == indexedContainer ?
                    this.iteratorList(graph.vertices()) :
                    RocksIndex.queryVertexIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate().getValue())
                            .stream().filter(vertex -> HasContainer.testAll(vertex, this.hasContainers))
                            .collect(Collectors.<Vertex>toList()).iterator();
        }

    }

    @SuppressWarnings("EqualsBetweenInconvertibleTypes")
    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
        final Set<String> indexedKeys = ((RocksGraph) this.getTraversal().getGraph().get()).getIndexedKeys(indexedClass);

        return this.hasContainers.stream()
                .filter(c -> indexedKeys.contains(c.getKey()) && c.getPredicate().getBiPredicate().equals(Compare.eq))
                .findAny()
                .orElseGet(() -> null);
    }

    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    private <E extends Element> Iterator<E> iteratorList(final Iterator<E> iterator) {
        final List<E> list = new ArrayList<>();
        while (iterator.hasNext()) {
            final E e = iterator.next();
            if (HasContainer.testAll(e, this.hasContainers))
                list.add(e);
        }
        return list.iterator();
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return this.hasContainers;
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }
}
