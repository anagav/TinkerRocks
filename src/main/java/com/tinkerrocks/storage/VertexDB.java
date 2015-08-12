package com.tinkerrocks.storage;

import com.tinkerrocks.structure.ByteUtil;
import com.tinkerrocks.structure.RocksElement;
import com.tinkerrocks.structure.RocksGraph;
import com.tinkerrocks.structure.RocksVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ashishn on 8/5/15.
 */


public class VertexDB {


    public void close() {
        this.rocksDB.close();
    }

    public <V> void setProperty(byte[] id, String key, V value) {
        try {
            this.rocksDB.put(getColumn(VERTEX_COLUMNS.PROPERTIES),
                    ByteUtil.merge(id, StorageConstants.PROPERTY_SEPERATOR.getBytes(), key.getBytes()), String.valueOf(value).getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    public void addEdge(byte[] vertexId, byte[] edgeId, Vertex inVertex) throws RocksDBException {
        this.rocksDB.put(getColumn(VERTEX_COLUMNS.OUT_EDGES), ByteUtil.merge(vertexId, StorageConstants.PROPERTY_SEPERATOR.getBytes(), edgeId), (byte[]) inVertex.id());
        this.rocksDB.put(getColumn(VERTEX_COLUMNS.IN_EDGES), ByteUtil.merge((byte[]) inVertex.id(), StorageConstants.PROPERTY_SEPERATOR.getBytes(), edgeId), vertexId);
    }

    public Map<String, byte[]> getProperties(RocksElement rocksVertex, String[] propertyKeys) throws RocksDBException {
        Map<String, byte[]> results = new HashMap<>();

        if (propertyKeys == null || propertyKeys.length == 0) {
            RocksIterator rocksIterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.PROPERTIES));
            byte[] seek_key = ByteUtil.merge((byte[]) rocksVertex.id(), StorageConstants.PROPERTY_SEPERATOR.getBytes());
            for (rocksIterator.seek(seek_key); rocksIterator.isValid() && ByteUtil.startsWith(rocksIterator.key(), 0, seek_key);
                 rocksIterator.next()) {
                results.put(new String(ByteUtil.slice(rocksIterator.key(), seek_key.length, rocksIterator.key().length)),
                        rocksIterator.value());
            }
            return results;
        }

        for (String property : propertyKeys) {
            byte[] key = rocksDB.get(getColumn(VERTEX_COLUMNS.PROPERTIES), ByteUtil.merge((byte[]) rocksVertex.id(), StorageConstants.PROPERTY_SEPERATOR.getBytes(), property.getBytes()));
            results.put(property, key);
        }
        return results;
    }

    public void addProperty(byte[] id, String key, String value) throws RocksDBException {
        this.rocksDB.put(ByteUtil.merge(id, StorageConstants.PROPERTY_SEPERATOR.getBytes(), key.getBytes()), value.getBytes());
    }

    public List<byte[]> getEdgeIDs(byte[] id, Direction direction, String[] edgeLabels) {
        List<byte[]> edgeIds = new ArrayList<>(50);
        RocksIterator iterator = null;
        byte[] seek_key = ByteUtil.merge(id, StorageConstants.PROPERTY_SEPERATOR.getBytes());

        try {
            if (direction == Direction.BOTH || direction == Direction.IN) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.IN_EDGES));
                for (iterator.seek(seek_key); iterator.isValid() &&
                        ByteUtil.startsWith(iterator.key(), 0, seek_key); iterator.next()) {
                    System.out.println("IN seeked:" + new String(iterator.key()) + "  value:" + new String(iterator.value()));

                    edgeIds.add(ByteUtil.slice(iterator.key(), seek_key.length));
                }
            }
            if (direction == Direction.BOTH || direction == Direction.OUT) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.OUT_EDGES));
                for (iterator.seek(seek_key); iterator.isValid() &&
                        ByteUtil.startsWith(iterator.key(), 0, seek_key); iterator.next()) {
                    System.out.println("OUT seeked:" + new String(iterator.key()) + "  value:" + new String(iterator.value()));
                    edgeIds.add(ByteUtil.slice(iterator.key(), seek_key.length));
                }
            }
        } finally {
            if (iterator != null) {
                iterator.dispose();
            }
        }
        return edgeIds;
    }

    public RocksVertex vertex(byte[] id, RocksGraph rocksGraph) throws RocksDBException {
        return (RocksVertex) vertices(new ArrayList<byte[]>() {
            {
                add(id);
            }
        }, rocksGraph).get(0);
    }


    public enum VERTEX_COLUMNS {
        PROPERTIES("PROPERTIES"),
        OUT_EDGES("OUT_EDGES"),
        IN_EDGES("IN_EDGES");

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
                    new ColumnFamilyOptions()));
        }
        this.rocksDB = RocksDB.open(new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true), "/tmp/vertices", columnFamilyDescriptors, columnFamilyHandleList);
    }


    public ColumnFamilyHandle getColumn(VERTEX_COLUMNS vertex_column) {
        return columnFamilyHandleList.get(vertex_column.ordinal() + 1);
    }

    public void addVertex(byte[] idValue, String label, Object[] keyValues) throws RocksDBException {

        this.rocksDB.put(idValue, label.getBytes());
        Map<String, Object> properties = ElementHelper.asMap(keyValues);
        for (Map.Entry<String, Object> property : properties.entrySet()) {
            this.rocksDB.put(getColumn(VERTEX_COLUMNS.PROPERTIES), ByteUtil.merge(idValue, StorageConstants.PROPERTY_SEPERATOR.getBytes(), property.getKey().getBytes()), String.valueOf(property.getValue()).getBytes());
        }
    }


    public List<Vertex> vertices(List<byte[]> vertexIds, RocksGraph rocksGraph) throws RocksDBException {
        List<Vertex> vertices = new ArrayList<>();

        if (vertexIds == null) {
            RocksIterator iterator = this.rocksDB.newIterator();
            iterator.seekToFirst();
            while (iterator.isValid()) {
                vertices.add(getVertex(iterator.key(), rocksGraph));
                iterator.next();
            }
        } else {
            for (byte[] vertexid : vertexIds) {
                vertices.add(getVertex(vertexid, rocksGraph));
            }
        }
        return vertices;
    }

    private RocksVertex getVertex(byte[] vertexId, RocksGraph rocksGraph) throws RocksDBException {
        return new RocksVertex(vertexId, getLabel(vertexId), rocksGraph);
    }


    private String getLabel(byte[] vertexid) throws RocksDBException {
        return new String(this.rocksDB.get(vertexid));
    }


}
