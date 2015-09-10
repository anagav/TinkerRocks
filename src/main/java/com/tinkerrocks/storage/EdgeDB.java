package com.tinkerrocks.storage;

import com.tinkerrocks.structure.RocksEdge;
import com.tinkerrocks.structure.RocksElement;
import com.tinkerrocks.structure.RocksGraph;
import com.tinkerrocks.structure.Utils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * <p>
 * class that handles edges. Including serialization and de-serialization.
 * </p>
 * Created by ashishn on 8/5/15.
 */


public class EdgeDB extends StorageAbstractClass implements EdgeStorage {


    public <V> void setProperty(byte[] id, String key, V value) {
        try {
            put(getColumn(EDGE_COLUMNS.PROPERTIES),
                    Utils.merge(id, StorageConstants.PROPERTY_SEPARATOR, key.getBytes()), serialize(value));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    public void addEdge(byte[] edge_id, String label, byte[] inVertex, byte[] outVertex, Object[] keyValues)
            throws Exception {
        //todo temp disable edge check
//        if (this.rocksDB.get(edge_id) != null) {
//            throw Graph.Exceptions.edgeWithIdAlreadyExists(edge_id);
//        }
        if (label == null) {
            throw Edge.Exceptions.labelCanNotBeNull();
        }
        if (label.isEmpty()) {
            throw Edge.Exceptions.labelCanNotBeEmpty();
        }
        put(edge_id, label.getBytes());

        put(getColumn(EDGE_COLUMNS.IN_VERTICES), Utils.merge(edge_id,
                StorageConstants.PROPERTY_SEPARATOR, inVertex), inVertex);

        put(getColumn(EDGE_COLUMNS.OUT_VERTICES), Utils.merge(edge_id,
                StorageConstants.PROPERTY_SEPARATOR, outVertex), outVertex);

        Map<String, Object> properties = ElementHelper.asMap(keyValues);
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            setProperty(edge_id, entry.getKey(), entry.getValue());
        }
    }

    public List<byte[]> getVertexIDs(byte[] edgeId, Direction direction) {
        List<byte[]> vertexIDs = new ArrayList<>(5);
        RocksIterator rocksIterator;

        byte[] seek_key = Utils.merge(edgeId, StorageConstants.PROPERTY_SEPARATOR);

        try {
            if (direction == Direction.BOTH || direction == Direction.IN) {
                rocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.IN_VERTICES));

                Utils.RocksIterUtil(rocksIterator, seek_key, (key, value) -> {
                    vertexIDs.add(Utils.slice(key, seek_key.length));
                    return true;
                });
            }
            if (direction == Direction.BOTH || direction == Direction.OUT) {
                rocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.OUT_VERTICES));
                Utils.RocksIterUtil(rocksIterator, seek_key, (key, value) -> {
                    vertexIDs.add(Utils.slice(key, seek_key.length));
                    return true;
                });
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return vertexIDs;
    }

    @Override
    public List<byte[]> getVertexIDs(List<byte[]> edgeIds, Direction direction) {
        List<byte[]> vertexIDs = new ArrayList<>(150);
        RocksIterator inRocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.IN_VERTICES), StorageConfigFactory.getReadOptions());
        RocksIterator outRocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.OUT_VERTICES), StorageConfigFactory.getReadOptions());

        try {
            for (byte[] edgeId : edgeIds) {
                byte[] seek_key = Utils.merge(edgeId, StorageConstants.PROPERTY_SEPARATOR);

                if (direction == Direction.BOTH || direction == Direction.IN) {
                    Utils.RocksIterUtil(inRocksIterator, false, seek_key, (key, value) -> {
                        vertexIDs.add(Utils.slice(key, seek_key.length));
                        return true;
                    });
                }
                if (direction == Direction.BOTH || direction == Direction.OUT) {
                    Utils.RocksIterUtil(outRocksIterator, false, seek_key, (key, value) -> {
                        vertexIDs.add(Utils.slice(key, seek_key.length));
                        return true;
                    });
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (inRocksIterator != null)
                inRocksIterator.dispose();
            if (outRocksIterator != null)
                outRocksIterator.dispose();
        }
        return vertexIDs;
    }

    public Map<String, Object> getProperties(RocksElement element, String[] propertyKeys) throws Exception {
        Map<String, Object> results = new HashMap<>();

        if (propertyKeys == null || propertyKeys.length == 0) {
            RocksIterator rocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.PROPERTIES));
            byte[] seek_key = Utils.merge((byte[]) element.id(), StorageConstants.PROPERTY_SEPARATOR);

            Utils.RocksIterUtil(rocksIterator, seek_key, (key, value) -> {
                results.put(new String(Utils.slice(key, seek_key.length, key.length)),
                        deserialize(value, Object.class));
                return true;
            });
            return results;
        }

        for (String property : propertyKeys) {
            byte[] val = get(getColumn(EDGE_COLUMNS.PROPERTIES),
                    Utils.merge((byte[]) element.id(), StorageConstants.PROPERTY_SEPARATOR,
                            property.getBytes()));
            if (val != null)
                results.put(property, deserialize(val, Object.class));
        }
        return results;
    }

    public List<Edge> edges(List<byte[]> ids, RocksGraph rocksGraph) throws Exception {
        List<Edge> edges = new ArrayList<>();
        if (ids.size() == 0) {
            RocksIterator iterator = this.rocksDB.newIterator();
            iterator.seekToFirst();
            while (iterator.isValid()) {
                edges.add(getEdge(iterator.key(), rocksGraph));
                iterator.next();
            }
            return edges;
        }
        Map<byte[], byte[]> results = this.rocksDB.multiGet(ids);
        return results.entrySet().stream().map(result -> new RocksEdge(result.getKey(),
                new String(result.getValue()), rocksGraph)).filter(rocksEdge -> rocksEdge != null).collect(Collectors.toList());
        //edges = ids.stream().map(bytes -> getEdge(bytes, rocksGraph)).filter(r -> r != null).collect(Collectors.toList());
    }

    @Override
    public RocksEdge getEdge(byte[] id, RocksGraph rocksGraph) {
        try {
            return new RocksEdge(id, getLabel(id), rocksGraph);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void remove(RocksEdge rocksEdge) throws RocksDBException {
        this.rocksDB.remove((byte[]) rocksEdge.id());
    }


    public enum EDGE_COLUMNS {
        PROPERTIES("PROPERTIES"),
        IN_VERTICES("IN_VERTICES"),
        OUT_VERTICES("OUT_VERTICES");

        String value;

        EDGE_COLUMNS(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }


    public EdgeDB(RocksGraph rocksGraph) throws RocksDBException {
        super(rocksGraph);
        RocksDB.loadLibrary();

        columnFamilyDescriptors = new ArrayList<>(EDGE_COLUMNS.values().length);
        columnFamilyHandleList = new ArrayList<>(EDGE_COLUMNS.values().length);
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, StorageConfigFactory.getColumnFamilyOptions()));
        for (EDGE_COLUMNS vertex_columns : EDGE_COLUMNS.values()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(vertex_columns.getValue().getBytes(),
                    StorageConfigFactory.getColumnFamilyOptions()));
        }
        this.rocksDB = RocksDB.open(StorageConfigFactory.getDBOptions(), getDbPath() + "/edges", columnFamilyDescriptors, columnFamilyHandleList);
        this.rocksDB.enableFileDeletions(true);
    }


    private ColumnFamilyHandle getColumn(EDGE_COLUMNS edge_column) {
        return columnFamilyHandleList.get(edge_column.ordinal() + 1);
    }


    public String getLabel(byte[] id) throws Exception {
        return new String(get(id));
    }

}
