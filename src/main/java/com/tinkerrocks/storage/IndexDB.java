package com.tinkerrocks.storage;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Created by ashishn on 8/13/15.
 */
public class IndexDB extends StorageAbstractClass {
    RocksDB rocksDB;

    public IndexDB() throws RocksDBException {
        rocksDB = RocksDB.open("/tmp/indices");

    }

    public void close() {

    }
}
