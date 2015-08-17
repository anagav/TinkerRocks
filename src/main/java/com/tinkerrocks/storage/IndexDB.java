package com.tinkerrocks.storage;

import com.google.common.base.Preconditions;
import com.tinkerrocks.structure.ByteUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by ashishn on 8/13/15.
 */
public class IndexDB extends StorageAbstractClass {

    public enum INDEX_COLUMNS {
        INDEX_KEYS("INDEX_KEYS");

        String value;

        INDEX_COLUMNS(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }


    RocksDB rocksDB;
    List<ColumnFamilyHandle> columnFamilyHandleList;
    List<ColumnFamilyDescriptor> columnFamilyDescriptors;


    public IndexDB() throws RocksDBException {

        columnFamilyDescriptors = new ArrayList<>(INDEX_COLUMNS.values().length);
        columnFamilyHandleList = new ArrayList<>(INDEX_COLUMNS.values().length);
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        for (INDEX_COLUMNS vertex_columns : INDEX_COLUMNS.values()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(vertex_columns.getValue().getBytes(),
                    StorageConfigFactory.getColumnFamilyOptions()));
        }

        rocksDB = RocksDB.open(StorageConfigFactory.getDBOptions(), StorageConstants.DATABASE_PREFIX + "/indexes", columnFamilyDescriptors, columnFamilyHandleList);
    }


    void put(byte[] key, byte[] value) throws RocksDBException {
        this.put(null, key, value);
    }

    void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
        if (columnFamilyHandle != null)
            this.rocksDB.put(columnFamilyHandle, StorageConfigFactory.getWriteOptions().setSync(true), key, value);
        else
            this.rocksDB.put(StorageConfigFactory.getWriteOptions().setSync(true), key, value);
    }


    public ColumnFamilyHandle getColumn(INDEX_COLUMNS edge_column) {
        return columnFamilyHandleList.get(edge_column.ordinal() + 1);
    }

    public void close() {
        this.rocksDB.close();
    }


    private String getIndexClass(Class _class) {
        return _class.getSimpleName();
    }

    public <T extends Element> void putIndex(Class<T> indexClass, String key, Object value, byte[] id) throws RocksDBException {
        Preconditions.checkNotNull(indexClass);
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(key);

        String className = getIndexClass(indexClass);
        byte[] key1 = (className +
                StorageConstants.PROPERTY_SEPERATOR + key + StorageConstants.PROPERTY_SEPERATOR + value).getBytes();

        key1 = ByteUtil.merge(key1, StorageConstants.PROPERTY_SEPERATOR.getBytes(), id);
        put(getColumn(INDEX_COLUMNS.INDEX_KEYS), (className +
                StorageConstants.PROPERTY_SEPERATOR + key).getBytes(), "".getBytes());
        put(key1, id);
    }


    public <T extends Element> List<byte[]> getIndex(Class<T> indexClass, String key, Object value) {
        List<byte[]> results = new ArrayList<>();
        RocksIterator iterator = this.rocksDB.newIterator();
        byte[] seek_key = (getIndexClass(indexClass) + StorageConstants.PROPERTY_SEPERATOR + key +
                StorageConstants.PROPERTY_SEPERATOR + value + StorageConstants.PROPERTY_SEPERATOR).getBytes();

        iterator.seek(seek_key);
        while (iterator.isValid() && ByteUtil.startsWith(iterator.key(), 0, seek_key)) {
            results.add(ByteUtil.slice(iterator.key(), seek_key.length));
            iterator.next();
        }
        return results;
    }


    public <T extends Element> void dropIndex(Class<T> indexClass, String key) throws RocksDBException {
        byte[] seek_key = (getIndexClass(indexClass) + StorageConstants.PROPERTY_SEPERATOR + key).getBytes();
        this.rocksDB.remove(getColumn(INDEX_COLUMNS.INDEX_KEYS), seek_key);
    }


    public <T extends Element> Set<String> getIndexedKeys(Class<T> indexClass) {
        Set<String> indexes = new HashSet<>();
        RocksIterator iterator = this.rocksDB.newIterator(getColumn(INDEX_COLUMNS.INDEX_KEYS));
        byte[] seek_key = (getIndexClass(indexClass) + StorageConstants.PROPERTY_SEPERATOR).getBytes();
        iterator.seek(seek_key);
        for (; iterator.isValid() && ByteUtil.startsWith(iterator.key(), 0, seek_key); iterator.next()) {
            indexes.add(new String(ByteUtil.slice(iterator.key(), seek_key.length)));
        }
        return indexes;
    }
}
