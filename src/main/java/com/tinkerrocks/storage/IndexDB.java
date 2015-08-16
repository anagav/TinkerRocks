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
    Set<String> indexes = new HashSet<>();


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

    public ColumnFamilyHandle getColumn(INDEX_COLUMNS edge_column) {
        return columnFamilyHandleList.get(edge_column.ordinal() + 1);
    }

    public void close() {
        this.rocksDB.close();
    }

    public <T extends Element> void putIndex(Class<T> indexClass, String key, Object value, byte[] id) throws RocksDBException {
        Preconditions.checkNotNull(indexClass);
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(key);

        String className = indexClass.getName();
        byte[] key1 = (className +
                StorageConstants.PROPERTY_SEPERATOR + key + StorageConstants.PROPERTY_SEPERATOR + value).getBytes();

        key1 = ByteUtil.merge(key1, StorageConstants.PROPERTY_SEPERATOR.getBytes(), id);
        this.rocksDB.put(getColumn(INDEX_COLUMNS.INDEX_KEYS), (className +
                StorageConstants.PROPERTY_SEPERATOR + key).getBytes(), "".getBytes());
        //indexes.add(indexClass.getName() + StorageConstants.PROPERTY_SEPERATOR + key + StorageConstants.PROPERTY_SEPERATOR + value);
        this.rocksDB.put(key1, id);
    }


    public <T extends Element> List<byte[]> getIndex(Class<T> indexClass, String key, Object value) {
        List<byte[]> results = new ArrayList<>();
        RocksIterator iterator = this.rocksDB.newIterator();
        byte[] seek_key = (indexClass.getName() + StorageConstants.PROPERTY_SEPERATOR + key +
                StorageConstants.PROPERTY_SEPERATOR + value).getBytes();

        iterator.seek(seek_key);
        while (iterator.isValid() && ByteUtil.startsWith(iterator.key(), 0, seek_key)) {
            results.add(ByteUtil.slice(iterator.key(), seek_key.length));
            iterator.next();
        }

        System.out.println("index size:" + results.size());
        return results;
    }


    public <T extends Element> void dropIndex(Class<T> indexClass, String key) throws RocksDBException {
        byte[] seek_key = (indexClass.getName() + StorageConstants.PROPERTY_SEPERATOR + key).getBytes();
        this.rocksDB.remove(getColumn(INDEX_COLUMNS.INDEX_KEYS), seek_key);
    }


    public <T extends Element> Set<String> getIndexedKeys(Class<T> indexClass) {

        RocksIterator iterator = this.rocksDB.newIterator(getColumn(INDEX_COLUMNS.INDEX_KEYS));
        byte[] seek_key = (indexClass.getName() + StorageConstants.PROPERTY_SEPERATOR).getBytes();
        iterator.seek(seek_key);
        for (; iterator.isValid() && ByteUtil.startsWith(iterator.key(), 0, seek_key); iterator.next()) {
            indexes.add(indexClass.getName() + StorageConstants.PROPERTY_SEPERATOR
                    + new String(ByteUtil.slice(iterator.key(), seek_key.length)));
        }
        System.out.println("indexes size" + indexes.size());

        return indexes;
    }
}
