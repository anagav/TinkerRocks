package com.tinkerrocks.structure;

import org.rocksdb.RocksDBException;

/**
 * Created by ashishn on 8/17/15.
 */
public interface RocksIteratorCallback {
    boolean process(byte[] key, byte[] value) throws RocksDBException;
}
