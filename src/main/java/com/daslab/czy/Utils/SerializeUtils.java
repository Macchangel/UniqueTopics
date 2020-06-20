package com.daslab.czy.Utils;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class SerializeUtils {
    static Kryo kryo = null;
    static{
        kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
    }

    public static byte[] serialize(Object obj) {
        kryo.register(obj.getClass());
        byte[] buffer = new byte[102400];
        Output output = new Output(buffer);
        kryo.writeObject(output, obj);
        byte[] bs = output.toBytes();
        output.close();
        return bs;
    }

    public static Object deserialize(byte[] src, Class<?> clazz) {
        kryo.register(clazz);
        Input input = new Input(src);
        Object obj = kryo.readObject(input, clazz);
        input.close();
        return obj;
    }
}
