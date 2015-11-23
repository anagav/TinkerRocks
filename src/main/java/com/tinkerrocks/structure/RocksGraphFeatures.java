package com.tinkerrocks.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Created by ashishn on 11/12/15.
 */
public class RocksGraphFeatures implements Graph.Features {

    @Override
    public GraphFeatures graph() {
        return new GraphFeatures() {


            @Override
            public boolean supportsComputer() {
                return false;
            }

            @Override
            public boolean supportsPersistence() {
                return true;
            }

            @Override
            public boolean supportsConcurrentAccess() {
                return true;
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
            public VariableFeatures variables() {
                return new VariableFeatures() {

                    @Override
                    public boolean supportsMapValues() {
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
                    public boolean supportsStringValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsUniformListValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsSerializableValues() {
                        return false;
                    }
                };
            }
        };
    }

    @Override
    public VertexFeatures vertex() {
        return new RocksVertexFeatures();
    }

    @Override
    public EdgeFeatures edge() {
        return new RocksEdgeFeatures();
    }
}
