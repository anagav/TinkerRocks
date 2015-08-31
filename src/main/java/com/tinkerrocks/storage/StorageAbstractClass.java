package com.tinkerrocks.storage;

import com.tinkerrocks.structure.RocksGraph;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.apache.tinkerpop.shaded.kryo.pool.KryoFactory;
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.UUID;

/**
 * Created by ashishn on 8/13/15.
 */
public abstract class StorageAbstractClass {


    protected KryoPool pool;
    protected RocksGraph rocksGraph;

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


}

