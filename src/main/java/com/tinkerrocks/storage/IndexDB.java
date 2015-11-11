package com.tinkerrocks.storage;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tinkerrocks.structure.RocksGraph;
import com.tinkerrocks.structure.Utils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * <p>
 * Class for handling indexes -> Creation, Update and deleting index keys
 * </p>
 * Created by ashishn on 8/13/15.
 */
public class IndexDB extends StorageAbstractClass implements IndexStorage {

    public <T extends Element> void removeIndex(Class<T> indexClass, String key, Object value, byte[] id) {
        try {
            this.rocksDB.remove((getIndexClass(indexClass) + Byte.toString(StorageConstants.PROPERTY_SEPARATOR) +
                    key + Byte.toString(StorageConstants.PROPERTY_SEPARATOR) + value + Byte.toString(StorageConstants.PROPERTY_SEPARATOR)
                    + new String(id)).getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public ColumnFamilyHandle getColumnForClass(Class indexClass) {
        if (Vertex.class.isAssignableFrom(indexClass)) {
            return getColumn(INDEX_COLUMNS.VERTEX_INDEX);
        }
        if (Edge.class.isAssignableFrom(indexClass)) {
            return getColumn(INDEX_COLUMNS.EDGE_INDEX);
        }
        throw new RuntimeException("indexing not supported for class of type:" + indexClass.getCanonicalName());
    }


    public void createIndex(Class indexClass, String key) {
        cache.invalidate(indexClass);

        try {
            put(getColumn(INDEX_COLUMNS.INDEX_KEYS), (getIndexClass(indexClass) +
                    Byte.toString(StorageConstants.PROPERTY_SEPARATOR) + key).getBytes(), "".getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

    }

    public enum INDEX_COLUMNS {
        INDEX_KEYS("INDEX_KEYS"),
        VERTEX_INDEX("VERTEX_INDEX"),
        EDGE_INDEX("VERTEX_INDEX");

        String value;

        INDEX_COLUMNS(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    private Cache<Class, Set<String>> cache;

    public IndexDB(RocksGraph rocksGraph) throws RocksDBException {
        super(rocksGraph);

        RocksDB.loadLibrary();

        columnFamilyDescriptors = new ArrayList<>(INDEX_COLUMNS.values().length);
        columnFamilyHandleList = new ArrayList<>(INDEX_COLUMNS.values().length);
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, StorageConfigFactory.getColumnFamilyOptions()));
        for (INDEX_COLUMNS vertex_columns : INDEX_COLUMNS.values()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(vertex_columns.getValue().getBytes(),
                    StorageConfigFactory.getColumnFamilyOptions()));
        }
        rocksDB = RocksDB.open(StorageConfigFactory.getDBOptions(), getDbPath() + "/indexes", columnFamilyDescriptors, columnFamilyHandleList);
        this.rocksDB.enableFileDeletions(true);

        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build();
    }


    public ColumnFamilyHandle getColumn(INDEX_COLUMNS edge_column) {
        return columnFamilyHandleList.get(edge_column.ordinal() + 1);
    }

    private String getIndexClass(Class _class) {
        return _class.getSimpleName();
    }


    @SuppressWarnings("unchecked")
    public <T extends Element> void putIndex(Class<T> indexClass, String key, Object value, byte[] id) throws Exception {
        Preconditions.checkNotNull(indexClass);
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(key);

        //String className = getIndexClass(indexClass);
        byte[] key1 = (key + StorageConstants.PROPERTY_SEPARATOR + value).getBytes();

        HashSet<byte[]> hashSet = (HashSet<byte[]>) deserialize(get(getColumnForClass(indexClass), key1), HashSet.class);
        if (hashSet == null) {
            hashSet = new HashSet<>();
        }
        hashSet.add(id);
        put(getColumnForClass(indexClass), key1, serialize(hashSet));
    }


    @SuppressWarnings("unchecked")
    public <T extends Element> List<byte[]> getIndex(Class<T> indexClass, String key, Object value) {
        List<byte[]> results = new ArrayList<>();
        try {
            byte[] key1 = (key + StorageConstants.PROPERTY_SEPARATOR + value).getBytes();
            HashSet<byte[]> hashSet = (HashSet<byte[]>) deserialize(get(getColumnForClass(indexClass), key1), HashSet.class);
            if (hashSet == null) {
                hashSet = new HashSet<>();
            }
            return hashSet.stream().collect(Collectors.toList());
        } catch (RocksDBException ex) {
            ex.printStackTrace();
        }
        return results;
    }


    public <T extends Element> void dropIndex(Class<T> indexClass, String key) throws RocksDBException {
        byte[] seek_key = (getIndexClass(indexClass) + Byte.toString(StorageConstants.PROPERTY_SEPARATOR) + key).getBytes();
        this.rocksDB.remove(getColumn(INDEX_COLUMNS.INDEX_KEYS), seek_key);
    }


    public <T extends Element> Set<String> getIndexedKeys(Class<T> indexClass) {

        try {
            return cache.get(indexClass, () -> {
                Set<String> indexes = new HashSet<>();
                RocksIterator iterator = rocksDB.newIterator(getColumn(INDEX_COLUMNS.INDEX_KEYS));
                byte[] seek_key = (getIndexClass(indexClass) + Byte.toString(StorageConstants.PROPERTY_SEPARATOR)).getBytes();
                try {
                    Utils.rocksIterUtil(iterator, seek_key, (key, value) -> {
                        indexes.add(new String(Utils.slice(key, seek_key.length)));
                        return true;
                    });
                } catch (Exception ex) {
                    throw new ExecutionException(ex);
                }
                return indexes;
            });
        } catch (ExecutionException e) {
            e.printStackTrace();
            cache.invalidate(indexClass);
        }
        return Collections.emptySet();
    }
}
