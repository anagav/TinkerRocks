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

    @SuppressWarnings("unchecked")
    public <V> void setProperty(byte[] id, String key, V value, VertexProperty.Cardinality cardinality) {
        byte[] record_key = Utils.merge(id, StorageConstants.PROPERTY_SEPARATOR, key.getBytes());
        try {
            if (cardinality == VertexProperty.Cardinality.single) {
                put(getColumn(VERTEX_COLUMNS.PROPERTIES), record_key, serialize(value));
                put(getColumn(VERTEX_COLUMNS.PROPERTY_TYPE), record_key, StorageConstants.V_PROPERTY_SINGLE_TYPE);
            }
            if (cardinality == VertexProperty.Cardinality.list || cardinality == VertexProperty.Cardinality.set) {
                byte[] oldData = get(getColumn(VERTEX_COLUMNS.PROPERTIES), record_key);
                byte[] oldType = get(getColumn(VERTEX_COLUMNS.PROPERTY_TYPE), record_key);
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
    public <V> List<VertexProperty<V>> getProperties(RocksElement rocksVertex, String[] propertyKeys) throws Exception {
        List<VertexProperty<V>> results = new LinkedList<>();

        if (propertyKeys.length == 0) {
            RocksIterator rocksIterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.PROPERTIES));
            byte[] seek_key = Utils.merge((byte[]) rocksVertex.id(), StorageConstants.PROPERTY_SEPARATOR);
            Utils.RocksIterUtil(rocksIterator, seek_key, (key, value) -> {
                if (value != null) {
                    byte[] property = Utils.slice(key, seek_key.length, key.length);
                    results.add(new RocksVertexProperty<>(rocksVertex, new String(property), (V) deserialize(value, Object.class)));
                }
                return true;
            });
            return results;
        }
        for (String property : propertyKeys) {
            byte[] lookup_key = Utils.merge((byte[]) rocksVertex.id(), StorageConstants.PROPERTY_SEPARATOR, property.getBytes());
            byte[] type = get(getColumn(VERTEX_COLUMNS.PROPERTY_TYPE), lookup_key);
            byte[] value = get(getColumn(VERTEX_COLUMNS.PROPERTIES), lookup_key);

            if (Utils.compare(type, StorageConstants.V_PROPERTY_SINGLE_TYPE)) {
                results.add(new RocksVertexProperty<>(rocksVertex, property, (V) deserialize(value, Object.class)));
            }
            if (Utils.compare(type, StorageConstants.V_PROPERTY_LIST_TYPE)) {
                List<V> values = deserialize(value, List.class);
                results.addAll(values.stream().map(inner_value ->
                        new RocksVertexProperty<>(rocksVertex, property, inner_value)).collect(Collectors.toList()));
            }
        }
        return results;
    }

    public List<byte[]> getEdgeIDs(byte[] id, Direction direction, String[] edgeLabels) {
        List<byte[]> edgeIds = new ArrayList<>();
        RocksIterator iterator;
        byte[] seek_key = Utils.merge(id, StorageConstants.PROPERTY_SEPARATOR);

        try {
            if (edgeLabels.length > 0) {
                RocksIterator inRocksIterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.IN_EDGES));
                RocksIterator outRocksIterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.OUT_EDGES));

                for (String edgeLabel : edgeLabels) {
                    byte[] inner_seek_key = Utils.merge(seek_key, edgeLabel.getBytes(), StorageConstants.PROPERTY_SEPARATOR);
                    if (direction == Direction.BOTH || direction == Direction.IN) {
                        Utils.RocksIterUtil(inRocksIterator, false, inner_seek_key, (key, value) -> {
                            edgeIds.add(Utils.slice(key, inner_seek_key.length));
                            return true;
                        });
                    }
                    if (direction == Direction.BOTH || direction == Direction.OUT) {
                        Utils.RocksIterUtil(outRocksIterator, false, inner_seek_key, (key, value) -> {
                            edgeIds.add(Utils.slice(key, inner_seek_key.length));
                            return true;
                        });
                    }
                }
                if (inRocksIterator != null) {
                    inRocksIterator.dispose();
                }
                if (outRocksIterator != null) {
                    outRocksIterator.dispose();
                }
                return edgeIds;
            }


            if (direction == Direction.BOTH || direction == Direction.IN) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.IN_EDGES));
                Utils.RocksIterUtil(iterator, seek_key, (key, value) -> {
                    byte[] edgeId = Utils.slice(key, Utils.findLastInArray(key, StorageConstants.PROPERTY_SEPARATOR));
                    edgeIds.add(edgeId);
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

        } catch (Exception ex) {
            ex.printStackTrace();
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
        // + 1 bcoz rocksdb requires opening default cf as first in the list.
        return columnFamilyHandleList.get(vertex_column.ordinal() + 1);
    }

    public void addVertex(byte[] idValue, String label, Object[] keyValues) throws Exception {
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
        return (get(idValue) != null);
    }


    public List<Vertex> vertices(List<byte[]> vertexIds, RocksGraph rocksGraph) throws RocksDBException {

        if (vertexIds == null || vertexIds.size() == 0) {
            return Collections.emptyList();
        }

        Map<byte[], byte[]> keys = this.rocksDB.multiGet(vertexIds);
        return keys.entrySet().stream().map(entry -> getVertex(entry.getKey(), entry.getValue(), rocksGraph))
                .collect(Collectors.toList());

    }

    private RocksVertex getVertex(byte[] key, byte[] value, RocksGraph rocksGraph) {
        return new RocksVertex(key, new String(value), rocksGraph);
    }


    public RocksVertex getVertex(byte[] vertexId, RocksGraph rocksGraph) {
        try {
            return new RocksVertex(vertexId, getLabel(vertexId), rocksGraph);
        } catch (RocksDBException ex) {
            ex.printStackTrace();
        }

        return null;
    }


    public String getLabel(byte[] vertexId) throws RocksDBException {
        byte[] result = get(vertexId);
        if (result == null) {
            throw Graph.Exceptions.elementNotFound(Vertex.class, new String(vertexId));
        }
        return new String(result);
    }

    @Override
    public List<byte[]> getEdgeVertexIDs(byte[] id, Direction direction, String[] edgeLabels) {
        List<byte[]> vertexIds = new ArrayList<>();
        RocksIterator iterator;
        byte[] seek_key = Utils.merge(id, StorageConstants.PROPERTY_SEPARATOR);

        try {
            if (edgeLabels.length > 0) {
                RocksIterator inRocksIterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.IN_EDGES));
                RocksIterator outRocksIterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.OUT_EDGES));

                for (String edgeLabel : edgeLabels) {
                    byte[] inner_seek_key = Utils.merge(seek_key, edgeLabel.getBytes(), StorageConstants.PROPERTY_SEPARATOR);
                    if (direction == Direction.BOTH || direction == Direction.IN) {
                        Utils.RocksIterUtil(inRocksIterator, false, inner_seek_key, (key, value) -> {
                            vertexIds.add(value);
                            return true;
                        });
                    }
                    if (direction == Direction.BOTH || direction == Direction.OUT) {
                        Utils.RocksIterUtil(outRocksIterator, false, inner_seek_key, (key, value) -> {
                            vertexIds.add(value);
                            return true;
                        });
                    }
                }
                if (inRocksIterator != null) {
                    inRocksIterator.dispose();
                }
                if (outRocksIterator != null) {
                    outRocksIterator.dispose();
                }
                return vertexIds;
            }


            if (direction == Direction.BOTH || direction == Direction.IN) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.IN_EDGES));
                Utils.RocksIterUtil(iterator, seek_key, (key, value) -> {
                    vertexIds.add(value);
                    return true;
                });
            }
            if (direction == Direction.BOTH || direction == Direction.OUT) {
                iterator = this.rocksDB.newIterator(getColumn(VERTEX_COLUMNS.OUT_EDGES));
                Utils.RocksIterUtil(iterator, seek_key, (key, value) -> {
                    vertexIds.add(value);
                    return true;
                });

            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return vertexIds;
    }


}
