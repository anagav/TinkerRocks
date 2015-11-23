package com.tinkerrocks.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Created by ashishn on 11/12/15.
 */
public class RocksVertexFeatures implements Graph.Features.VertexFeatures {

    @Override
    public boolean supportsAddVertices() {
        return true;
    }

    @Override
    public boolean supportsRemoveVertices() {
        return false;
    }

    @Override
    public boolean supportsMultiProperties() {
        return true;
    }

    @Override
    public boolean supportsMetaProperties() {
        return true;
    }

    @Override
    public Graph.Features.VertexPropertyFeatures properties() {
        return new RocksVertexPropertyFeatures();
    }

    @Override
    public boolean supportsCustomIds() {
        return false;
    }

    @Override
    public boolean supportsNumericIds() {
        return false;
    }

    @Override
    public boolean supportsAnyIds() {
        return false;
    }

    @Override
    public boolean supportsAddProperty() {
        return true;
    }

    @Override
    public boolean supportsRemoveProperty() {
        return false;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
        return true;
    }

    @Override
    public boolean supportsUuidIds() {
        return false;
    }
}
