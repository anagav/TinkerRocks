package com.tinkerrocks.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Created by ashishn on 11/12/15.
 */
public class RocksEdgeFeatures implements Graph.Features.EdgeFeatures {
    @Override
    public boolean supportsAddEdges() {
        return true;
    }

    @Override
    public boolean supportsRemoveEdges() {
        return false;
    }

    @Override
    public Graph.Features.EdgePropertyFeatures properties() {
        return new RocksEdgePropertyFeatures();
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
        return false;
    }

    @Override
    public boolean supportsNumericIds() {
        return false;
    }

    @Override
    public boolean supportsStringIds() {
        return true;
    }

    @Override
    public boolean supportsUuidIds() {
        return false;
    }



    @Override
    public boolean supportsCustomIds() {
        return false;
    }

    @Override
    public boolean supportsAnyIds() {
        return false;
    }

    @Override
    public boolean willAllowId(Object id) {
        return false;
    }
}
