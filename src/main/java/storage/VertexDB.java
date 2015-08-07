package storage;

import com.tinkerrocks.ByteUtil;
import com.tinkerrocks.RocksElement;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.rocksdb.*;

import java.util.*;

/**
 * Created by ashishn on 8/5/15.
 */


public class VertexDB {
    public static final String PROPERTY_SEPERATOR = "#";


    public void close() {
        this.rocksDB.close();
    }

    public <V> void setProperty(String id, String key, V value) {
        try {
            this.rocksDB.put(getColumn(VERTEX_COLUMNS.PROPERTIES), (id + key).getBytes(), String.valueOf(value).getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    public void addEdge(String vertexId, String edgeId, String inVertexID) throws RocksDBException {
        this.rocksDB.put(getColumn(VERTEX_COLUMNS.OUT_EDGES),
                (vertexId + PROPERTY_SEPERATOR + edgeId).getBytes(), inVertexID.getBytes());
    }

    public Map<String, byte[]> getProperties(RocksElement rocksVertex, String[] propertyKeys) throws RocksDBException {
        Map<String, byte[]> results = new HashMap<>();

        if (propertyKeys == null || propertyKeys.length == 0) {
            RocksIterator rocksIterator = this.rocksDB.newIterator();
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

    public Vertex addVertex(Object idValue, String label, Object[] keyValues) {
        return null;
    }


    public Iterator<Vertex> vertices(Vertex[] vertexIds) {
        return null;
    }


}
