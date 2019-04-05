package com.accor.tech.serdes;

import com.accor.tech.domain.EventActionStream;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class GenericSerdes {

    public static Serde<JsonNode> jsonNodeSerde() {
        return Serdes.serdeFrom(new JsonNodeSerialializer(), new JsonNodeDeserializer());
    }

    public static Serde<EventActionStream> eventActionStreamSerde() {
        return Serdes.serdeFrom(new EventActionStreamSerialializer(), new GenericDeserializer<>(EventActionStream.class));
    }
}
