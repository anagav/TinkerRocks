package com.tinkerrocks.storage;

/**
 * Created by ashishn on 8/11/15.
 */
public class StorageConstants {
    public static final String PROPERTY_SEPARATOR = "#";
    public static final String DATABASE_PREFIX = System.getProperty("user.home") + "/graph_data/databases";
    public static final String TEST_DATABASE_PREFIX = "/tmp/databases";
    public static final byte[] V_PROPERTY_LIST_TYPE = "LIST".getBytes();
    public static final byte[] V_PROPERTY_SINGLE_TYPE = "SINGLE".getBytes();
}
