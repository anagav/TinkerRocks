package com.tinkerrocks.structure;

/**
 * Created by ashishn on 8/17/15.
 */
public interface RocksIteratorCallback {
    boolean process(byte[] key, byte[] value);
}
