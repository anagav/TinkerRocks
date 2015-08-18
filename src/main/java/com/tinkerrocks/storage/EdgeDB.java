package com.tinkerrocks.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tinkerrocks.structure.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * <p>
 * class that handles edges. Including serialization and de-serialization.
 * </p>
 * Created by ashishn on 8/5/15.
 */


public class EdgeDB extends StorageAbstractClass {


    public void close() {
        this.rocksDB.close();
    }

    public <V> void setProperty(String id, String key, V value) {
        try {
            put(getColumn(EDGE_COLUMNS.PROPERTIES), (id + key).getBytes(), serialize(value));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    void put(byte[] key, byte[] value) throws RocksDBException {
        this.put(null, key, value);
    }

    void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
        if (columnFamilyHandle != null)
            this.rocksDB.put(columnFamilyHandle, StorageConfigFactory.getWriteOptions(), key, value);
        else
            this.rocksDB.put(StorageConfigFactory.getWriteOptions(), key, value);
    }


    public void addEdge(byte[] edge_id, String label, RocksElement inVertex, RocksElement outVertex, Object[] keyValues) throws RocksDBException {
        //todo add check back when finished testing
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
                StorageConstants.PROPERTY_SEPERATOR.getBytes(), (byte[]) inVertex.id()), (byte[]) inVertex.id());

        put(getColumn(EDGE_COLUMNS.OUT_VERTICES), Utils.merge(edge_id,
                StorageConstants.PROPERTY_SEPERATOR.getBytes(), (byte[]) outVertex.id()), (byte[]) outVertex.id());

        Map<String, Object> properties = ElementHelper.asMap(keyValues);
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            put(getColumn(EDGE_COLUMNS.PROPERTIES),
                    Utils.merge(edge_id, StorageConstants.PROPERTY_SEPERATOR.getBytes(), entry.getKey().getBytes()),
                    serialize(entry.getValue()));
        }
    }

    public List<byte[]> getVertexIDs(byte[] edgeId, Direction direction) {
        List<byte[]> vertexIDs = new ArrayList<>(16);
        RocksIterator rocksIterator;
        byte[] seek_key = Utils.merge(edgeId, StorageConstants.PROPERTY_SEPERATOR.getBytes());
        if (direction == Direction.BOTH || direction == Direction.IN) {
            rocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.IN_VERTICES));

            Utils.RocksIterUtil(rocksIterator, seek_key, (key, value) -> {
                vertexIDs.add(Utils.slice(value, seek_key.length));
                return true;
            });
        }
        if (direction == Direction.BOTH || direction == Direction.OUT) {
            rocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.OUT_VERTICES));
            Utils.RocksIterUtil(rocksIterator, seek_key, (key, value) -> {
                vertexIDs.add(Utils.slice(value, seek_key.length));
                return true;
            });
        }
        return vertexIDs;
    }

    public Map<String, Object> getProperties(RocksElement element, String[] propertyKeys) throws RocksDBException {
        Map<String, Object> results = new HashMap<>();

        if (propertyKeys == null || propertyKeys.length == 0) {
            RocksIterator rocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.PROPERTIES));
            byte[] seek_key = Utils.merge((byte[]) element.id(), StorageConstants.PROPERTY_SEPERATOR.getBytes());

            Utils.RocksIterUtil(rocksIterator, seek_key, (key, value) -> {
                results.put(new String(Utils.slice(key, seek_key.length, key.length)),
                        deserialize(value));

                return true;
            });


//            for (rocksIterator.seek(seek_key); rocksIterator.isValid() && Utils.startsWith(rocksIterator.key(), 0, seek_key);
//                 rocksIterator.next()) {
//                results.put(new String(Utils.slice(rocksIterator.key(), seek_key.length, rocksIterator.key().length)),
//                        deserialize(rocksIterator.value()));
//            }
            return results;
        }

        for (String property : propertyKeys) {


            byte[] val = rocksDB.get(getColumn(EDGE_COLUMNS.PROPERTIES),
                    Utils.merge((byte[]) element.id(), StorageConstants.PROPERTY_SEPERATOR.getBytes(),
                            property.getBytes()));


            if (val != null)
                results.put(property, deserialize(val));
            else
                results.put(property, null);

        }
        return results;
    }

    public List<Edge> edges(List<byte[]> ids, RocksGraph rocksGraph) throws RocksDBException {
        List<Edge> edges = new ArrayList<>();
        if (ids.size() == 0) {
            RocksIterator iterator = this.rocksDB.newIterator();
            iterator.seekToFirst();
            while (iterator.isValid()) {
                edges.add(getEdge(iterator.key(), rocksGraph));
                iterator.next();
            }
        }


        for (byte[] id : ids) {
            edges.add(getEdge(id, rocksGraph));
        }
        return edges;
    }


    public RocksEdge getEdge(byte[] id, RocksGraph rocksGraph) throws RocksDBException {

        try {
            return edgeCache.get(id, () -> {
                byte[] in_vertex_id = getVertex(id, Direction.IN);
                byte[] out_vertex_id = getVertex(id, Direction.OUT);

                RocksVertex inVertex = rocksGraph.getStorageHandler().getVertexDB().vertex(in_vertex_id, rocksGraph);
                RocksVertex outVertex = rocksGraph.getStorageHandler().getVertexDB().vertex(out_vertex_id, rocksGraph);

                return new RocksEdge(id, getLabel(id), rocksGraph, inVertex, outVertex);
            });
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw Vertex.Exceptions.edgeAdditionsNotSupported();
        }
    }

    private byte[] getVertex(byte[] id, Direction direction) {
        RocksIterator iterator;
        if (direction == Direction.BOTH || direction == Direction.OUT)
            iterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.OUT_VERTICES));
        else {
            iterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.IN_VERTICES));
        }

        byte[] seek_key = Utils.merge(id, StorageConstants.PROPERTY_SEPERATOR.getBytes());

        final byte[][] returnValue = new byte[1][1];
        Utils.RocksIterUtil(iterator, seek_key, (key, value) -> {
            returnValue[0] = Utils.slice(iterator.key(), seek_key.length);
            return false;
        });

        return returnValue[0];

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


    RocksDB rocksDB;
    List<ColumnFamilyHandle> columnFamilyHandleList;
    List<ColumnFamilyDescriptor> columnFamilyDescriptors;
    Cache<byte[], RocksEdge> edgeCache;


    public EdgeDB() throws RocksDBException {
        columnFamilyDescriptors = new ArrayList<>(EDGE_COLUMNS.values().length);
        columnFamilyHandleList = new ArrayList<>(EDGE_COLUMNS.values().length);
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        for (EDGE_COLUMNS vertex_columns : EDGE_COLUMNS.values()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(vertex_columns.getValue().getBytes(),
                    StorageConfigFactory.getColumnFamilyOptions()));
        }
        this.rocksDB = RocksDB.open(StorageConfigFactory.getDBOptions(), StorageConstants.DATABASE_PREFIX + "/edges", columnFamilyDescriptors, columnFamilyHandleList);
        this.edgeCache = CacheBuilder.newBuilder().maximumSize(1000000).concurrencyLevel(1000).build();

    }


    public ColumnFamilyHandle getColumn(EDGE_COLUMNS edge_column) {
        return columnFamilyHandleList.get(edge_column.ordinal() + 1);
    }


    public String getLabel(byte[] id) throws RocksDBException {
        return new String(this.rocksDB.get(id));
    }


}
