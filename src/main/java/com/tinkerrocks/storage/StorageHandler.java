package com.tinkerrocks.storage;

import com.tinkerrocks.structure.RocksGraph;
import org.rocksdb.RocksDBException;

/**
 * <p>handles storage </p>
 * Created by ashishn on 8/4/15.
 */
public class StorageHandler {
    VertexDB vertexDB;
    EdgeDB edgeDB;
    IndexDB indexDB;

    public StorageHandler(RocksGraph rocksGraph) throws RocksDBException {
        vertexDB = new VertexDB(rocksGraph);
        edgeDB = new EdgeDB(rocksGraph);
        indexDB = new IndexDB(rocksGraph);
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
