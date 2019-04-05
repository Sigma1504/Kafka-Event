package com.accor.tech;

import com.accor.tech.api.StreamingRequest;
import com.accor.tech.serdes.JsonNodeSerialializer;
import com.accor.tech.utils.JSONUtils;
import com.accor.tech.utilstest.DataExample;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class StreamWithProcessorTest extends AbstractKafkaEventDrivenTest {

    @Test
    public void testAddStreamWithProcessor() {

        String topicIn = "topic-in";
        String topicOut = "topic-out";
        ConsumerRecordFactory<String, JsonNode> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new JsonNodeSerialializer());

        final StreamingRequest request = StreamingRequest.builder()
                .streamName("TestStreamWithProcessor")
                .topicsIn(Collections.singleton(topicIn))
                .topicOut(Collections.singleton(topicOut))
                .beforeFilter((k, v) -> "ToProcess".equals(k))
                .processor(new MyProcessor())
                .build();

        streamApi.createStream(request);

        driver = getDriver();

        //GenerateData
        List<KeyValue<String, JsonNode>> listData = new ArrayList<>();
        listData.add(new KeyValue("ToProcess", JSONUtils.getInstance().toJsonNode(new DataExample("gnii", "1"))));
        listData.add(new KeyValue("ToProcess", JSONUtils.getInstance().toJsonNode(new DataExample("nothing", "1"))));
        listData.add(new KeyValue("ToProcess", JSONUtils.getInstance().toJsonNode(new DataExample("gnaa", "1"))));
        listData.add(new KeyValue("Noprocess", JSONUtils.getInstance().toJsonNode(new DataExample("sdsdsdsd", "1"))));
        listData.add(new KeyValue("ToProcess", JSONUtils.getInstance().toJsonNode(new DataExample("nothing", "1"))));
        listData.add(new KeyValue("Noprocess", JSONUtils.getInstance().toJsonNode(new DataExample("gnee", "1"))));

        driver.pipeInput(recordFactory.create(topicIn, listData));


        OutputVerifier.compareKeyValue(driver.readOutput(topicOut, stringDeserializer, jsonNodeDeserializer),
                "ToProcess", JSONUtils.getInstance().toJsonNode(new DataExample("gnii_processed", "1_processed")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOut, stringDeserializer, jsonNodeDeserializer),
                "ToProcess", JSONUtils.getInstance().toJsonNode(new DataExample("nothing_processed", "1_processed")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOut, stringDeserializer, jsonNodeDeserializer),
                "ToProcess", JSONUtils.getInstance().toJsonNode(new DataExample("gnaa_processed", "1_processed")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOut, stringDeserializer, jsonNodeDeserializer),
                "ToProcess", JSONUtils.getInstance().toJsonNode(new DataExample("nothing_processed", "1_processed")));
        Assert.assertNull(driver.readOutput(topicOut, stringDeserializer, jsonNodeDeserializer));
    }

    class MyProcessor extends AbstractProcessor<String, JsonNode> {

        @Override
        public void process(String key, JsonNode value) {
            String data = JSONUtils.getInstance().at(value, "myData").asText() + "_processed";
            String blabla = JSONUtils.getInstance().at(value, "blabla").asText() + "_processed";
            JSONUtils.getInstance().put(value, "myData", JsonNodeFactory.instance.textNode(data));
            JSONUtils.getInstance().put(value, "blabla", JsonNodeFactory.instance.textNode(blabla));
        }
    }


}
