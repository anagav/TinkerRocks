package com.tinkerrocks.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Created by ashishn on 11/12/15.
 */
public class RocksVertexPropertyFeatures implements Graph.Features.VertexPropertyFeatures {
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
    public boolean supportsNumericIds() {
        return true;
    }

    @Override
    public boolean supportsStringIds() {
        return true;
    }

    @Override
    public boolean supportsUuidIds() {
        return true;
    }

    @Override
    public boolean supportsCustomIds() {
        return false;
    }

    @Override
    public boolean supportsAnyIds() {
        return true;
    }

    @Override
    public boolean supportsMapValues() {
        return false;
    }

    @Override
    public boolean supportsMixedListValues() {
        return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
        return false;
    }

    @Override
    public boolean supportsByteArrayValues() {
        return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
        return false;
    }

    @Override
    public boolean supportsFloatArrayValues() {
        return false;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
        return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
        return false;
    }

    @Override
    public boolean supportsLongArrayValues() {
        return false;
    }

    @Override
    public boolean supportsSerializableValues() {
        return false;
    }

    @Override
    public boolean supportsStringValues() {
        return true;
    }

    @Override
    public boolean supportsUniformListValues() {
        return false;
    }
}
