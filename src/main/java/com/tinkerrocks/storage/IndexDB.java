package com.tinkerrocks.storage;

import com.google.common.base.Preconditions;
import com.tinkerrocks.structure.ByteUtil;
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

    public <T extends Element> void putIndex(Class<T> indexClass, String key, Object value, byte[] id) throws RocksDBException {
        Preconditions.checkNotNull(indexClass);
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);

        String className = indexClass.getName();
        byte[] key1 = (className +
                StorageConstants.PROPERTY_SEPERATOR + key + StorageConstants.PROPERTY_SEPERATOR + value).getBytes();
        key1 = ByteUtil.merge(key1, id);
        this.rocksDB.put(key1, id);
    }




}
