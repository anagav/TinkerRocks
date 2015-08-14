package com.tinkerrocks.storage;

import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.apache.tinkerpop.shaded.kryo.pool.KryoFactory;
import org.apache.tinkerpop.shaded.kryo.pool.KryoPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Created by ashishn on 8/13/15.
 */
public abstract class StorageAbstractClass {

    protected KryoPool pool;

    public StorageAbstractClass() {
        KryoFactory factory = () -> {
            Kryo kryo = new Kryo();
            kryo.register(Integer.class);
            kryo.register(String.class);
            return kryo;
        };
        pool = new KryoPool.Builder(factory).softReferences().build();
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


    public Object deserialize(byte[] inbBytes) {
        return pool.run(kryo -> {
            Input input = new Input(new ByteArrayInputStream(inbBytes));
            return kryo.readClassAndObject(input);
        });
    }


}

