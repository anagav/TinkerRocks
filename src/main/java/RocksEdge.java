import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by ashishn on 8/4/15.
 */
public class RocksEdge extends RocksElement implements Edge {

    protected final Vertex inVertex;
    protected final Vertex outVertex;

    public RocksEdge(Object id, String label, Vertex inVertex, Vertex outVertex) {
        super(id, label);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex);
            case IN:
                return IteratorUtils.of(this.inVertex);
            default:
                return IteratorUtils.of(this.outVertex, this.inVertex);
        }
    }

    @Override
    public Set<String> keys() {
        return null;
    }

    @Override
    public Graph graph() {
        return this.inVertex.graph();
    }

    @Override
    public <V> Property<V> property(String s, V v) {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<Property<V>> properties(String... strings) {
        return null;
    }
}
