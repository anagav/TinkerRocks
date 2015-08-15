package com.tinkerrocks.storage;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Created by ashishn on 8/13/15.
 */
public class IndexDB extends StorageAbstractClass {
    RocksDB rocksDB;

    public IndexDB() throws RocksDBException {
        rocksDB = RocksDB.open("/tmp/indexes");

    }

    public void close() {
        this.rocksDB.close();
    }




    public <T extends Element> void putVertexIndex(Class<T> indexClass, String key, Object value, Object id) {

    }
}
