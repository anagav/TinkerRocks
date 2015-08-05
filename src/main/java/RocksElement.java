/**
 * Created by ashishn on 8/4/15.
 */

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public abstract class RocksElement implements Element {

    protected final Object id;
    protected final String label;
    protected boolean removed = false;

    protected RocksElement(final Object id, final String label) {
        this.id = id;
        this.label = label;
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

}
