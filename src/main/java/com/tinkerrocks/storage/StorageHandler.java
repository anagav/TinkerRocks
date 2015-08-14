package com.tinkerrocks.storage;

import org.rocksdb.RocksDBException;

/**
 * <p>handles storage </p>
 * Created by ashishn on 8/4/15.
 */
public class StorageHandler {
    VertexDB vertexDB;
    EdgeDB edgeDB;
    IndexDB indexDB;

    public StorageHandler() throws RocksDBException {
        vertexDB = new VertexDB();
        edgeDB = new EdgeDB();
        indexDB = new IndexDB();
    }


    public VertexDB getVertexDB() {
        return vertexDB;
    }

    public EdgeDB getEdgeDB() {
        return edgeDB;
    }

    public IndexDB getIndexDB() {
        return indexDB;
    }

    public void close() {
        vertexDB.close();
        edgeDB.close();
        indexDB.close();
    }


}
