package storage;

import com.tinkerrocks.ByteUtil;
import com.tinkerrocks.RocksElement;
import com.tinkerrocks.RocksVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ashishn on 8/5/15.
 */


public class VertexDB {
    public static final String PROPERTY_SEPERATOR = "#";


    public void close() {
        this.rocksDB.close();
    }

    public <V> void setProperty(byte[] id, String key, V value) {
        try {
            this.rocksDB.put(getColumn(VERTEX_COLUMNS.PROPERTIES), (id + key).getBytes(), String.valueOf(value).getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    public void addEdge(String vertexId, Edge edge, Vertex inVertex) throws RocksDBException {
        this.rocksDB.put(getColumn(VERTEX_COLUMNS.OUT_EDGES),
                (vertexId + PROPERTY_SEPERATOR + edge.id()).getBytes(), String.valueOf(inVertex.id()).getBytes());
    }

    public Map<String, byte[]> getProperties(RocksElement rocksVertex, String[] propertyKeys) throws RocksDBException {
        Map<String, byte[]> results = new HashMap<>();

        if (propertyKeys == null || propertyKeys.length == 0) {
            RocksIterator rocksIterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.PROPERTIES));
            byte[] seek_key = (rocksVertex.id() + PROPERTY_SEPERATOR).getBytes();
            for (rocksIterator.seek(seek_key); rocksIterator.isValid() && ByteUtil.startsWith(rocksIterator.key(), 0, seek_key);
                 rocksIterator.next()) {
                results.put(new String(ByteUtil.slice(rocksIterator.key(), seek_key.length, rocksIterator.key().length)),
                        rocksIterator.value());
            }
            return results;
        }

        for (String property : propertyKeys) {
            byte[] val = rocksDB.get((rocksVertex.id() + PROPERTY_SEPERATOR + property).getBytes());
            if (val != null)
                results.put(property, val);
        }
        return results;
    }

    public void addProperty(String id, String key, String value) throws RocksDBException {
        this.rocksDB.put((id + PROPERTY_SEPERATOR + key).getBytes(), value.getBytes());
    }

    public List<byte[]> getEdgeIDs(String id, Direction direction, String[] edgeLabels) {
        List<byte[]> edgeIds = new ArrayList<>(50);
        RocksIterator iterator = null;
        byte[] seek_key = (id + PROPERTY_SEPERATOR).getBytes();

        try {
            if (direction == Direction.BOTH || direction == Direction.IN) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.IN_EDGES));
                for (iterator.seek((id + PROPERTY_SEPERATOR).getBytes()); iterator.isValid() &&
                        ByteUtil.startsWith(iterator.key(), 0, seek_key); iterator.next()) {
                    edgeIds.add(iterator.value());
                }
            }
            if (direction == Direction.BOTH || direction == Direction.OUT) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.OUT_EDGES));
                for (iterator.seek((id + PROPERTY_SEPERATOR).getBytes()); iterator.isValid() &&
                        ByteUtil.startsWith(iterator.key(), 0, seek_key); iterator.next()) {
                    edgeIds.add(iterator.value());
                }
            }
        } finally {
            if (iterator != null) {
                iterator.dispose();
            }
        }
        return edgeIds;
    }

    public RocksVertex vertex(byte[] id) {
        return (RocksVertex) vertices(new ArrayList<byte[]>() {
            {
                add(id);
            }
        }).get(0);
    }


    public static enum VERTEX_COLUMNS {
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
        for (VERTEX_COLUMNS vertex_columns : VERTEX_COLUMNS.values()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(vertex_columns.getValue().getBytes(),
                    new ColumnFamilyOptions()));
        }
        this.rocksDB = RocksDB.open(new DBOptions(), "/tmp/vertices", columnFamilyDescriptors, columnFamilyHandleList);
    }


    public ColumnFamilyHandle getColumn(VERTEX_COLUMNS vertex_column) {
        return columnFamilyHandleList.get(vertex_column.ordinal());
    }

    public void addVertex(Object idValue, String label, Object[] keyValues) throws RocksDBException {


        this.rocksDB.put(String.valueOf(idValue).getBytes(), label.getBytes());
        Map<String, Object> properties = ElementHelper.asMap(keyValues);
        byte[] id = ByteUtil.merge(String.valueOf(idValue).getBytes(), PROPERTY_SEPERATOR.getBytes());
        for (Map.Entry<String, Object> property : properties.entrySet()) {
            this.rocksDB.put(ByteUtil.merge(id, property.getKey().getBytes()), String.valueOf(property.getValue()).getBytes());
        }
    }


    public List<Vertex> vertices(List<byte[]> vertexIds) {
        return null;
    }


}
