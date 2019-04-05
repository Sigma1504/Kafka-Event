package com.accor.tech.serdes;

import com.accor.tech.utils.JSONUtils;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;
@Slf4j
public class JsonNodeDeserializer implements Deserializer<JsonNode> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public JsonNode deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return JSONUtils.getInstance().parseWithError(new String(bytes));
        } catch (IOException e) {
            log.error("Deserilisation failed for {}",s);
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {

    }
}
