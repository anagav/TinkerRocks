package storage;

import com.tinkerrocks.ByteUtil;
import com.tinkerrocks.RocksElement;
import com.tinkerrocks.RocksProperty;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


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

    public <V> Iterator<Property<V>> getPropertiesIterator(RocksElement rocksElement, String id, String[] propertyKeys) {
        RocksIterator rocksIterator = this.rocksDB.newIterator(getColumn(EDGE_COLUMNS.PROPERTIES));
        List<Property<V>> results = new ArrayList<>(100);
        for (String property : propertyKeys) {
            rocksIterator.seek((id + VertexDB.PROPERTY_SEPERATOR + property).getBytes());
            if (rocksIterator.isValid() && ByteUtil.startsWith(rocksIterator.key(), 0, property.getBytes())) {
                results.add(new RocksProperty<V>(rocksElement, property, (V) new String(rocksIterator.value())));
            }
        }
        return results.iterator();
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
        for (EDGE_COLUMNS vertex_columns : EDGE_COLUMNS.values()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(vertex_columns.getValue().getBytes(),
                    new ColumnFamilyOptions()));
        }
        this.rocksDB = RocksDB.open(new DBOptions(), "/tmp/edges", columnFamilyDescriptors, columnFamilyHandleList);
    }


    public ColumnFamilyHandle getColumn(EDGE_COLUMNS edge_column) {
        return columnFamilyHandleList.get(edge_column.ordinal());
    }


    public Iterator<Edge> edges(Object[] edgeIds) {
        return null;
    }

}
