package com.tinkerrocks.storage;

import com.tinkerrocks.structure.RocksGraph;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.apache.tinkerpop.shaded.kryo.pool.KryoFactory;
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

/**
 * Created by ashishn on 8/13/15.
 */
public abstract class StorageAbstractClass {


    protected KryoPool pool;
    protected RocksGraph rocksGraph;
    RocksDB rocksDB;
    List<ColumnFamilyHandle> columnFamilyHandleList;
    List<ColumnFamilyDescriptor> columnFamilyDescriptors;


    public StorageAbstractClass(RocksGraph rocksGraph) {
        KryoFactory factory = () -> {
            Kryo kryo = new Kryo();
            kryo.register(Integer.class);
            kryo.register(HashSet.class);
            kryo.register(UUID.class);
            kryo.register(String.class);
            kryo.register(ArrayList.class);
            return kryo;
        };
        pool = new KryoPool.Builder(factory).softReferences().build();
        this.rocksGraph = rocksGraph;
    }

    public byte[] serialize(Object inObject) {
        return pool.run(kryo -> {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(128);
            Output output = new Output(byteArrayOutputStream);
            kryo.writeClassAndObject(output, inObject);
            output.flush();
            output.close();
            return byteArrayOutputStream.toByteArray();
        });
    }


    private Object deserialize(byte[] inbBytes) {

        if (inbBytes == null) {
            return null;
        }
        return pool.run(kryo -> {
            Input input = new Input(new ByteArrayInputStream(inbBytes));
            return kryo.readClassAndObject(input);
        });
    }

    public <T> T deserialize(byte[] inbBytes, Class<T> _clazz) {
        Object data = deserialize(inbBytes);
        if (data == null) {
            return null;
        }

        return _clazz.cast(deserialize(inbBytes));
    }

    protected String getDbPath() {
        return rocksGraph.getConfiguration().getString(StorageConstants.STORAGE_DIR_PROPERTY);
    }


    protected void put(byte[] key, byte[] value) throws Exception {
        this.put(null, key, value);
    }

    protected void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
        if (columnFamilyHandle != null)
            this.rocksDB.put(columnFamilyHandle, StorageConfigFactory.getWriteOptions(), key, value);
        else
            this.rocksDB.put(StorageConfigFactory.getWriteOptions(), key, value);
    }


    protected byte[] get(byte[] key) throws RocksDBException {
        return this.get(null, key);
    }

    protected byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        if (columnFamilyHandle != null)
            return this.rocksDB.get(columnFamilyHandle, StorageConfigFactory.getReadOptions(), key);
        else
            return this.rocksDB.get(StorageConfigFactory.getReadOptions(), key);
    }


    public void close() {
        if (rocksDB != null) {
            this.rocksDB.dispose();
            this.rocksDB.close();
        }
    }


}

