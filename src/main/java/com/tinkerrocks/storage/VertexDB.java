package com.tinkerrocks.storage;

import com.tinkerrocks.structure.RocksElement;
import com.tinkerrocks.structure.RocksGraph;
import com.tinkerrocks.structure.RocksVertex;
import com.tinkerrocks.structure.Utils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ashishn on 8/5/15.
 */


public class VertexDB extends StorageAbstractClass {


    public void close() {
        if (rocksDB != null)
            this.rocksDB.close();
    }

    public <V> void setProperty(byte[] id, String key, V value) {
        try {
            put(getColumn(VERTEX_COLUMNS.PROPERTIES),
                    Utils.merge(id, StorageConstants.PROPERTY_SEPERATOR.getBytes(), key.getBytes()), serialize(value));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    public void addEdge(byte[] vertexId, Edge edge, Vertex inVertex) throws RocksDBException {
        put(getColumn(VERTEX_COLUMNS.OUT_EDGES), Utils.merge(vertexId,
                StorageConstants.PROPERTY_SEPERATOR.getBytes(), (byte[]) edge.id()), (byte[]) inVertex.id());
        put(getColumn(VERTEX_COLUMNS.IN_EDGES), Utils.merge((byte[]) inVertex.id(),
                StorageConstants.PROPERTY_SEPERATOR.getBytes(), (byte[]) edge.id()), vertexId);
        put(getColumn(VERTEX_COLUMNS.OUT_EDGE_LABELS), (byte[]) edge.id(), edge.label().getBytes());
    }

    public Map<String, Object> getProperties(RocksElement rocksVertex, String[] propertyKeys) throws RocksDBException {
        Map<String, Object> results = new HashMap<>();

        if (propertyKeys == null || propertyKeys.length == 0) {
            RocksIterator rocksIterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.PROPERTIES));
            byte[] seek_key = Utils.merge((byte[]) rocksVertex.id(), StorageConstants.PROPERTY_SEPERATOR.getBytes());
            Utils.RocksIterUtil(rocksIterator, seek_key, (key, value) -> {
                if (value != null)
                    results.put(new String(Utils.slice(key, seek_key.length, key.length)),
                            deserialize(value));
                return true;
            });
            return results;
        }

        for (String property : propertyKeys) {
            byte[] key = rocksDB.get(getColumn(VERTEX_COLUMNS.PROPERTIES),
                    Utils.merge((byte[]) rocksVertex.id(), StorageConstants.PROPERTY_SEPERATOR.getBytes(),
                            property.getBytes()));
            results.put(property, deserialize(key));
        }
        return results;
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

    public <V> void addProperty(byte[] id, String key, V value) throws RocksDBException {
        put(Utils.merge(id, StorageConstants.PROPERTY_SEPERATOR.getBytes(), key.getBytes()), serialize(value));
    }

    public List<byte[]> getEdgeIDs(byte[] id, Direction direction, HashSet<byte[]> edgeLabels) {
        List<byte[]> edgeIds = new ArrayList<>(50);
        RocksIterator iterator = null;
        byte[] seek_key = Utils.merge(id, StorageConstants.PROPERTY_SEPERATOR.getBytes());
        Set<byte[]> results = new HashSet<>(edgeLabels.size());

        results.addAll(edgeLabels.stream().map(this::get).collect(Collectors.toSet()));


        try {
            if (direction == Direction.BOTH || direction == Direction.IN) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.IN_EDGES));
                Utils.RocksIterUtil(iterator, seek_key, (key, value) -> {
                    if (edgeLabels.size() == 0) {
                        edgeIds.add(Utils.slice(key, seek_key.length));
                    } else {
                        if (results.contains(key)) {
                            edgeIds.add(Utils.slice(key, seek_key.length));
                        }
                    }
                    return true;
                });
            }
            if (direction == Direction.BOTH || direction == Direction.OUT) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.OUT_EDGES));
                Utils.RocksIterUtil(iterator, seek_key, (key, value) -> {
                    if (edgeLabels.size() == 0) {
                        edgeIds.add(Utils.slice(key, seek_key.length));
                    } else {
                        if (results.contains(key)) {
                            edgeIds.add(Utils.slice(key, seek_key.length));
                        }
                    }
                    return true;
                });

            }

        } finally {
            if (iterator != null) {
                iterator.dispose();
            }
        }
        return edgeIds;
    }

    private byte[] get(byte[] bytes) {
        try {
            return this.rocksDB.get(bytes);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        return null;
    }

//    private boolean contains(String[] edgeLabels, byte[] s) throws RocksDBException {
//        for (String edge : edgeLabels) {
//            if (Arrays.equals(edge.getBytes(), this.rocksDB.get(getColumn(VERTEX_COLUMNS.OUT_EDGE_LABELS), s))) {
//                return true;
//            }
//        }
//        return false;
//    }

    public RocksVertex vertex(byte[] id, RocksGraph rocksGraph) throws RocksDBException {
        return (RocksVertex) vertices(new ArrayList<byte[]>() {
            {
                add(id);
            }
        }, rocksGraph).get(0);
    }

    public void remove(RocksVertex rocksVertex) throws RocksDBException {
        this.rocksDB.remove((byte[]) rocksVertex.id());
    }


    public enum VERTEX_COLUMNS {
        PROPERTIES("PROPERTIES"),
        OUT_EDGES("OUT_EDGES"),
        IN_EDGES("IN_EDGES"),
        OUT_EDGE_LABELS("OUT_EDGE_LABELS"),
        IN_EDGE_LABELS("IN_EDGE_LABELS");

        String value;

        VERTEX_COLUMNS(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    RocksDB rocksDB;
    List<ColumnFamilyHandle> columnFamilyHandleList;
    List<ColumnFamilyDescriptor> columnFamilyDescriptors;

    public VertexDB() throws RocksDBException {
        columnFamilyDescriptors = new ArrayList<>(VERTEX_COLUMNS.values().length);
        columnFamilyHandleList = new ArrayList<>(VERTEX_COLUMNS.values().length);
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        for (VERTEX_COLUMNS vertex_columns : VERTEX_COLUMNS.values()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(vertex_columns.getValue().getBytes(),
                    StorageConfigFactory.getColumnFamilyOptions()));
        }
        this.rocksDB = RocksDB.open(StorageConfigFactory.getDBOptions(), StorageConstants.DATABASE_PREFIX + "/vertices", columnFamilyDescriptors, columnFamilyHandleList);
    }


    public ColumnFamilyHandle getColumn(VERTEX_COLUMNS vertex_column) {
        return columnFamilyHandleList.get(vertex_column.ordinal() + 1);
    }

    public void addVertex(byte[] idValue, String label, Object[] keyValues) throws RocksDBException {

        put(idValue, label.getBytes());
        if (keyValues == null || keyValues.length == 0) {
            return;
        }
        Map<String, Object> properties = ElementHelper.asMap(keyValues);

        for (Map.Entry<String, Object> property : properties.entrySet()) {
            setProperty(idValue, property.getKey(), property.getValue());
        }
    }


    public List<Vertex> vertices(List<byte[]> vertexIds, RocksGraph rocksGraph) throws RocksDBException {
        List<Vertex> vertices;


        if (vertexIds == null) {
            RocksIterator iterator = this.rocksDB.newIterator();
            vertexIds = new ArrayList<>();
            iterator.seekToFirst();
            try {
                while (iterator.isValid()) {
                    vertexIds.add(iterator.key());
                    iterator.next();
                }
            } finally {
                iterator.dispose();
            }
        }

        vertices = vertexIds.stream().map(bytes -> getVertex(bytes, rocksGraph)).collect(Collectors.toList());

        return vertices;
    }


    public RocksVertex getVertex(byte[] vertexId, RocksGraph rocksGraph) {
        try {
            if (this.rocksDB.get(vertexId) == null) {
                return null;
            }
            return new RocksVertex(vertexId, getLabel(vertexId), rocksGraph);
        } catch (RocksDBException ex) {
            ex.printStackTrace();
        }
        return null;
    }


    public String getLabel(byte[] vertexid) throws RocksDBException {
        byte[] result = this.rocksDB.get(vertexid);
        if (result == null) {
            throw Graph.Exceptions.elementNotFound(Vertex.class, new String(vertexid));
        }
        return new String(result);
    }


}
