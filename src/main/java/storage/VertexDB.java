package storage;

import com.tinkerrocks.ByteUtil;
import com.tinkerrocks.RocksElement;
import com.tinkerrocks.RocksProperty;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    public <V> Iterator<Property<V>> getPropertiesIterator(RocksElement rocksElement, String id, String[] propertyKeys) {
        RocksIterator rocksIterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.PROPERTIES));
        List<Property<V>> results = new ArrayList<>(100);
        for (String property : propertyKeys) {
            rocksIterator.seek((id + PROPERTY_SEPERATOR + property).getBytes());
            if (rocksIterator.isValid() && ByteUtil.startsWith(rocksIterator.key(), 0, property.getBytes())) {
                results.add(new RocksProperty<V>(rocksElement, property, (V) new String(rocksIterator.value())));
            }
        }
        return results.iterator();
    }


    public void addEdge(String vertexId, String edgeId, String inVertexID) throws RocksDBException {
        this.rocksDB.put(getColumn(VERTEX_COLUMNS.OUT_EDGES),
                (vertexId + PROPERTY_SEPERATOR + edgeId).getBytes(), inVertexID.getBytes());
    }

    public static enum VERTEX_COLUMNS {
        PROPERTIES("properties"),
        OUT_EDGES("in_edges"),
        IN_EDGES("out_edges");

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
