package com.tinkerrocks.storage;

import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.List;
import java.util.Set;

/**
 * Created by ashishn on 8/28/15.
 */
public interface IndexStorage extends CommonStorage {

    <T extends Element> void removeIndex(Class<T> indexClass, String key, Object value, byte[] id);

    void createIndex(Class indexClass, String key);

    void put(byte[] key, byte[] value) throws Exception;

    <T extends Element> void putIndex(Class<T> indexClass, String key, Object value, byte[] id) throws Exception;

    <T extends Element> List<byte[]> getIndex(Class<T> indexClass, String key, Object value);

    <T extends Element> void dropIndex(Class<T> indexClass, String key) throws Exception;

    <T extends Element> Set<String> getIndexedKeys(Class<T> indexClass);


}
