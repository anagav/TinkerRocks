package com.tinkerrocks.storage;

import com.tinkerrocks.structure.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by ashishn on 8/5/15.
 */


public class EdgeDB {


    public void close() {
        this.rocksDB.close();
    }

    public <V> void setProperty(String id, String key, V value) {
        try {
            this.rocksDB.put(getColumn(EDGE_COLUMNS.PROPERTIES), (id + key).getBytes(), String.valueOf(value).getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
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

        this.rocksDB.put(edge_id, label.getBytes());
        this.rocksDB.put(getColumn(EDGE_COLUMNS.IN_VERTICES), ByteUtil.merge(edge_id,
                StorageConstants.PROPERTY_SEPERATOR.getBytes(), (byte[]) inVertex.id()), (byte[]) inVertex.id());

        this.rocksDB.put(getColumn(EDGE_COLUMNS.OUT_VERTICES), ByteUtil.merge(edge_id,
                StorageConstants.PROPERTY_SEPERATOR.getBytes(), (byte[]) outVertex.id()), (byte[]) outVertex.id());

        Map<String, Object> properties = ElementHelper.asMap(keyValues);
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            this.rocksDB.put(getColumn(EDGE_COLUMNS.PROPERTIES),
                    ByteUtil.merge(edge_id, StorageConstants.PROPERTY_SEPERATOR.getBytes(), entry.getKey().getBytes()),
                    String.valueOf(entry.getValue()).getBytes());
        }
    }

    public List<byte[]> getVertexIDs(byte[] edgeId, Direction direction) {
        List<byte[]> vertexIDs = new ArrayList<>(16);
        RocksIterator rocksIterator;
        byte[] seek_key = ByteUtil.merge(edgeId, StorageConstants.PROPERTY_SEPERATOR.getBytes());
        if (direction == Direction.BOTH || direction == Direction.IN) {
            rocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.IN_VERTICES));
            for (rocksIterator.seek(seek_key); rocksIterator.isValid()
                    && ByteUtil.startsWith(rocksIterator.key(), 0, seek_key); rocksIterator.next()) {
                vertexIDs.add(ByteUtil.slice(rocksIterator.value(), seek_key.length));
            }
        }
        if (direction == Direction.BOTH || direction == Direction.OUT) {
            rocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.OUT_VERTICES));
            for (rocksIterator.seek(seek_key); rocksIterator.isValid()
                    && ByteUtil.startsWith(rocksIterator.key(), 0, seek_key); rocksIterator.next()) {
                vertexIDs.add(ByteUtil.slice(rocksIterator.value(), seek_key.length));
            }
        }
        return vertexIDs;
    }

    public Map<String, byte[]> getProperties(RocksElement element, String[] propertyKeys) throws RocksDBException {
        Map<String, byte[]> results = new HashMap<>();

        if (propertyKeys == null || propertyKeys.length == 0) {
            RocksIterator rocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.PROPERTIES));
            byte[] seek_key = (element.id() + StorageConstants.PROPERTY_SEPERATOR).getBytes();
            for (rocksIterator.seek(seek_key); rocksIterator.isValid() && ByteUtil.startsWith(rocksIterator.key(), 0, seek_key);
                 rocksIterator.next()) {
                results.put(new String(ByteUtil.slice(rocksIterator.key(), seek_key.length, rocksIterator.key().length)),
                        rocksIterator.value());
            }
            return results;
        }

        for (String property : propertyKeys) {
            byte[] val = rocksDB.get((element.id() + StorageConstants.PROPERTY_SEPERATOR + property).getBytes());
            if (val != null)
                results.put(property, val);
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


    RocksEdge getEdge(byte[] id, RocksGraph rocksGraph) throws RocksDBException {
        byte[] in_vertex_id = getVertex(id, Direction.IN);
        byte[] out_vertex_id = getVertex(id, Direction.OUT);

        RocksVertex inVertex = rocksGraph.getStorageHandler().getVertexDB().vertex(in_vertex_id, rocksGraph);
        RocksVertex outVertex = rocksGraph.getStorageHandler().getVertexDB().vertex(out_vertex_id, rocksGraph);

        return new RocksEdge(id, getLabel(id), rocksGraph, inVertex, outVertex);

    }

    private byte[] getVertex(byte[] id, Direction direction) {
        RocksIterator iterator;
        if (direction == Direction.BOTH || direction == Direction.OUT)
            iterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.OUT_VERTICES));
        else {
            iterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.IN_VERTICES));
        }


        byte[] seek_key = ByteUtil.merge(id, StorageConstants.PROPERTY_SEPERATOR.getBytes());
        iterator.seek(seek_key);
        if (iterator.isValid() && ByteUtil.startsWith(iterator.key(), 0, seek_key)) {
            return ByteUtil.slice(iterator.key(), seek_key.length);
        }
        return null;
    }


    public static enum EDGE_COLUMNS {
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

    public EdgeDB() throws RocksDBException {
        columnFamilyDescriptors = new ArrayList<>(EDGE_COLUMNS.values().length);
        columnFamilyHandleList = new ArrayList<>(EDGE_COLUMNS.values().length);
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        for (EDGE_COLUMNS vertex_columns : EDGE_COLUMNS.values()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(vertex_columns.getValue().getBytes(),
                    new ColumnFamilyOptions()));
        }
        this.rocksDB = RocksDB.open(new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true), "/tmp/edges", columnFamilyDescriptors, columnFamilyHandleList);
    }


    public ColumnFamilyHandle getColumn(EDGE_COLUMNS edge_column) {
        return columnFamilyHandleList.get(edge_column.ordinal() + 1);
    }


    public String getLabel(byte[] id) throws RocksDBException {
        return new String(this.rocksDB.get(id));
    }


}
