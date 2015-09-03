package com.tinkerrocks.storage;

import com.tinkerrocks.structure.*;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.rocksdb.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ashishn on 8/5/15.
 */


public class VertexDB extends StorageAbstractClass implements VertexStorage {


    public void close() {
        if (rocksDB != null)
            this.rocksDB.close();
    }

    @SuppressWarnings("unchecked")
    public <V> void setProperty(byte[] id, String key, V value, VertexProperty.Cardinality cardinality) {
        byte[] record_key = Utils.merge(id, StorageConstants.PROPERTY_SEPARATOR, key.getBytes());
        try {
            if (cardinality == VertexProperty.Cardinality.single) {
                put(getColumn(VERTEX_COLUMNS.PROPERTIES), record_key, serialize(value));
                put(getColumn(VERTEX_COLUMNS.PROPERTY_TYPE), record_key, StorageConstants.V_PROPERTY_SINGLE_TYPE);
            }
            if (cardinality == VertexProperty.Cardinality.list || cardinality == VertexProperty.Cardinality.set) {
                byte[] oldData = this.rocksDB.get(getColumn(VERTEX_COLUMNS.PROPERTIES), record_key);
                byte[] oldType = this.rocksDB.get(getColumn(VERTEX_COLUMNS.PROPERTY_TYPE), record_key);
                ArrayList<V> results;
                if (!Utils.compare(oldType, StorageConstants.V_PROPERTY_LIST_TYPE)) {
                    results = new ArrayList<>();
                } else {
                    results = (ArrayList<V>) deserialize(oldData, ArrayList.class);
                }
                if (cardinality == VertexProperty.Cardinality.set && results.contains(value)) {
                    return;
                }
                results.add(value);
                put(getColumn(VERTEX_COLUMNS.PROPERTIES), record_key, serialize(results));
                put(getColumn(VERTEX_COLUMNS.PROPERTY_TYPE), record_key, StorageConstants.V_PROPERTY_LIST_TYPE);
            }

        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }


    public void addEdge(byte[] vertexId, Edge edge, Vertex inVertex) throws RocksDBException {

        byte[] insert_label = Utils.merge(StorageConstants.PROPERTY_SEPARATOR, edge.label().getBytes(), StorageConstants.PROPERTY_SEPARATOR);

        put(getColumn(VERTEX_COLUMNS.OUT_EDGES), Utils.merge(vertexId,
                insert_label, (byte[]) edge.id()), (byte[]) inVertex.id());

        put(getColumn(VERTEX_COLUMNS.IN_EDGES), Utils.merge((byte[]) inVertex.id(),
                insert_label, (byte[]) edge.id()), vertexId);
    }

    @SuppressWarnings("unchecked")
    public <V> List<VertexProperty<V>> getProperties(RocksElement rocksVertex, List<byte[]> propertyKeys) throws Exception {
        List<VertexProperty<V>> results = new LinkedList<>();
        if (propertyKeys == null) {
            propertyKeys = new ArrayList<>();
        }
        if (propertyKeys.size() == 0) {
            RocksIterator rocksIterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.PROPERTIES));
            byte[] seek_key = Utils.merge((byte[]) rocksVertex.id(), StorageConstants.PROPERTY_SEPARATOR);
            final List<byte[]> finalPropertyKeys = propertyKeys;
            Utils.RocksIterUtil(rocksIterator, seek_key, (key, value) -> {
                if (value != null)
                    finalPropertyKeys.add(Utils.slice(key, seek_key.length, key.length));
                return true;
            });
        }

        for (byte[] property : propertyKeys) {
            byte[] lookup_key = Utils.merge((byte[]) rocksVertex.id(), StorageConstants.PROPERTY_SEPARATOR, property);
            byte[] type = rocksDB.get(getColumn(VERTEX_COLUMNS.PROPERTY_TYPE), lookup_key);
            byte[] value = rocksDB.get(getColumn(VERTEX_COLUMNS.PROPERTIES), lookup_key);

            if (Utils.compare(type, StorageConstants.V_PROPERTY_SINGLE_TYPE)) {
                results.add(new RocksVertexProperty<>(rocksVertex, new String(property), (V) deserialize(value, Object.class)));
            }
            if (Utils.compare(type, StorageConstants.V_PROPERTY_LIST_TYPE)) {
                List<V> values = deserialize(value, List.class);
                results.addAll(values.stream().map(inner_value -> new RocksVertexProperty<>(rocksVertex, new String(property), inner_value))
                        .collect(Collectors.toList()));
            }
        }
        return results;
    }


    private void put(byte[] key, byte[] value) throws RocksDBException {
        this.put(null, key, value);
    }

    private void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
        if (columnFamilyHandle != null)
            this.rocksDB.put(columnFamilyHandle, StorageConfigFactory.getWriteOptions(), key, value);
        else
            this.rocksDB.put(StorageConfigFactory.getWriteOptions(), key, value);
    }

    private byte[] get(byte[] key) {
        try {
            return this.get(null, key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    private byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        if (key == null) {
            return null;
        }
        if (columnFamilyHandle != null)
            return this.rocksDB.get(columnFamilyHandle, StorageConfigFactory.getReadOptions(), key);
        else
            return this.rocksDB.get(StorageConfigFactory.getReadOptions(), key);
    }


    public List<byte[]> getEdgeIDs(byte[] id, Direction direction, HashSet<String> edgeLabels) {
        List<byte[]> edgeIds = new ArrayList<>();
        RocksIterator iterator;
        byte[] seek_key = Utils.merge(id, StorageConstants.PROPERTY_SEPARATOR);


        try {

            if (edgeLabels.size() > 0) {
                for (String edgeLabel : edgeLabels) {
                    byte[] inner_seek_key = Utils.merge(seek_key, edgeLabel.getBytes(), StorageConstants.PROPERTY_SEPARATOR);
                    if (direction == Direction.BOTH || direction == Direction.IN) {
                        iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.IN_EDGES));
                        Utils.RocksIterUtil(iterator, inner_seek_key, (key, value) -> {
                            edgeIds.add(Utils.slice(key, inner_seek_key.length));
                            return true;
                        });
                    }
                    if (direction == Direction.BOTH || direction == Direction.OUT) {
                        iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.OUT_EDGES));
                        Utils.RocksIterUtil(iterator, inner_seek_key, (key, value) -> {
                            edgeIds.add(Utils.slice(key, inner_seek_key.length));
                            return true;
                        });
                    }
                }
                return edgeIds;
            }


            if (direction == Direction.BOTH || direction == Direction.IN) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.IN_EDGES));
                Utils.RocksIterUtil(iterator, seek_key, (key, value) -> {
                    if (edgeLabels.size() == 0) {
                        edgeIds.add(Utils.slice(key, seek_key.length));
                    } else {
                        byte[] edgeId = Utils.slice(key, Utils.findLastInArray(key, StorageConstants.PROPERTY_SEPARATOR), seek_key.length);
                        String edgeLabel = this.rocksGraph.getStorageHandler().getEdgeDB().getLabel(edgeId);
                        if (edgeLabels.contains(edgeLabel)) {
                            edgeIds.add(edgeId);
                        }
                    }
                    return true;
                });
            }
            if (direction == Direction.BOTH || direction == Direction.OUT) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.OUT_EDGES));

                Utils.RocksIterUtil(iterator, seek_key, (key, value) -> {
                    byte[] edgeId = Utils.slice(key, Utils.findLastInArray(key, StorageConstants.PROPERTY_SEPARATOR));
                    edgeIds.add(edgeId);
                    return true;
                });

            }

        } catch (Exception ignored) {
            ignored.printStackTrace();
        }
        return edgeIds;
    }

    public RocksVertex vertex(byte[] id, RocksGraph rocksGraph) throws Exception {
        return getVertex(id, rocksGraph);
    }

    public void remove(RocksVertex rocksVertex) throws RocksDBException {
        this.rocksDB.remove((byte[]) rocksVertex.id());
    }


    public enum VERTEX_COLUMNS {
        PROPERTIES("PROPERTIES"),
        PROPERTY_TYPE("PROPERTY_TYPE"),
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

    public VertexDB(RocksGraph rocksGraph) throws RocksDBException {
        super(rocksGraph);
        RocksDB.loadLibrary();

        columnFamilyDescriptors = new ArrayList<>(VERTEX_COLUMNS.values().length);
        columnFamilyHandleList = new ArrayList<>(VERTEX_COLUMNS.values().length);
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, StorageConfigFactory.getColumnFamilyOptions()));
        for (VERTEX_COLUMNS vertex_columns : VERTEX_COLUMNS.values()) {
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(vertex_columns.getValue().getBytes(),
                    StorageConfigFactory.getColumnFamilyOptions()));
        }


        this.rocksDB = RocksDB.open(StorageConfigFactory.getDBOptions(), getDbPath() + "/vertices", columnFamilyDescriptors, columnFamilyHandleList);
        this.rocksDB.enableFileDeletions(true);

    }

    public ColumnFamilyHandle getColumn(VERTEX_COLUMNS vertex_column) {
        return columnFamilyHandleList.get(vertex_column.ordinal() + 1);
    }

    public void addVertex(byte[] idValue, String label, Object[] keyValues) throws RocksDBException {
        if (exists(idValue)) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(new String(idValue));
        }

        put(idValue, label.getBytes());
        if (keyValues == null || keyValues.length == 0) {
            return;
        }
        Map<String, Object> properties = ElementHelper.asMap(keyValues);

        for (Map.Entry<String, Object> property : properties.entrySet()) {
            setProperty(idValue, property.getKey(), property.getValue(), VertexProperty.Cardinality.single);
        }
    }

    private boolean exists(byte[] idValue) throws RocksDBException {
        return (this.rocksDB.get(idValue) != null);
    }


    public List<Vertex> vertices(List<byte[]> vertexIds, RocksGraph rocksGraph) throws RocksDBException {

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


        return vertexIds.stream().map(bytes -> getVertex(bytes, rocksGraph)).
                filter(rocksVertex -> rocksVertex != null).collect(Collectors.toList());
    }


    public RocksVertex getVertex(byte[] vertexId, RocksGraph rocksGraph) {
        try {
            if (rocksDB.get(vertexId) == null) {
                return null;
            }
            return new RocksVertex(vertexId, getLabel(vertexId), rocksGraph);
        } catch (RocksDBException ex) {
            ex.printStackTrace();
        }

        return null;
    }


    public String getLabel(byte[] vertexId) throws RocksDBException {
        byte[] result = this.rocksDB.get(vertexId);
        if (result == null) {
            throw Graph.Exceptions.elementNotFound(Vertex.class, new String(vertexId));
        }
        return new String(result);
    }


}
