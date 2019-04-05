package com.accor.tech.serdes;

import com.accor.tech.utils.JSONUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class GenericDeserializer<T> implements Deserializer<T> {

    private final Class<T> clazz;

    public GenericDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    public void configure(Map<String, ?> map, boolean b) {

    }

    public T deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        T objToReturn = null;
        try {
            objToReturn = JSONUtils.getInstance().parse(bytes, clazz);
        } catch (Exception e) {
            log.error("Deserilisation failed for {}", s);
            throw new SerializationException(e);
        }
        return objToReturn;
    }

    public void close() {

    }
}

