package com.tinkerrocks.storage;

import org.rocksdb.RocksDBException;

/**
 * <p>handles storage </p>
 * Created by ashishn on 8/4/15.
 */
public class StorageHandler {
    VertexDB vertexDB;
    EdgeDB edgeDB;

    public StorageHandler() throws RocksDBException {
        vertexDB = new VertexDB();
        edgeDB = new EdgeDB();
    }


    public VertexDB getVertexDB() {
        return vertexDB;
    }

    public EdgeDB getEdgeDB() {
        return edgeDB;
    }

    public void close() {
        vertexDB.close();
        edgeDB.close();
    }


}
