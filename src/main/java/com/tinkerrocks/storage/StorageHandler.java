package com.tinkerrocks.storage;

import com.tinkerrocks.structure.RocksGraph;
import org.rocksdb.RocksDBException;

/**
 * <p>handles storage </p>
 * Created by ashishn on 8/4/15.
 */
public class StorageHandler {
    VertexStorage vertexDB;
    EdgeStorage edgeDB;
    IndexStorage indexDB;

    public StorageHandler(RocksGraph rocksGraph) throws RocksDBException {
        vertexDB = new VertexDB(rocksGraph);
        edgeDB = new EdgeDB(rocksGraph);
        indexDB = new IndexDB(rocksGraph);
    }


    public VertexStorage getVertexDB() {
        return vertexDB;
    }

    public EdgeStorage getEdgeDB() {
        return edgeDB;
    }

    public IndexStorage getIndexDB() {
        return indexDB;
    }

    public void close() {
        vertexDB.close();
        edgeDB.close();
        indexDB.close();
    }


}
