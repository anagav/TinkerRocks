package storage;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ashishn on 8/5/15.
 */


public class VertexDB {


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
