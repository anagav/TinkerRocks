package com.tinkerrocks.storage;

/**
 * Created by ashishn on 8/11/15.
 */
public class StorageConstants {
    public static byte PROPERTY_SEPARATOR = '\006';
    public static final String TEST_DATABASE_PREFIX = "/tmp/databases";
    static final byte[] V_PROPERTY_LIST_TYPE = "LIST".getBytes();
    static final byte[] V_PROPERTY_SINGLE_TYPE = "SINGLE".getBytes();
    public static final String STORAGE_DIR_PROPERTY = "com.tinkerrocks.storage.dir";

}
