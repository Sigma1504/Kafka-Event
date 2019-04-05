package com.accor.tech.serdes;

import com.accor.tech.utils.JSONUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class GenericSerializer<T> implements Serializer<T> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, T objToSerialize) {
        byte[] retVal = null;
        try {
            retVal = JSONUtils.getInstance().asJsonString(objToSerialize).getBytes();
        } catch (JsonProcessingException e) {
            log.error("Can't serialize source " + objToSerialize, e);
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
