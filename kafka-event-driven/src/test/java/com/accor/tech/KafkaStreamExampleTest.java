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
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class KafkaStreamExampleTest extends AbstractKafkaEventDrivenTest {
    
    @Test
    public void testExample() {

        String streamName = "streamNameTest";
        String topicInput = "test-in-topic";
        String topicOutput = "test-out-topic";
        //Mock service

        final StreamingRequest request = StreamingRequest.builder()
                .streamName(streamName)
                .topicsIn(Collections.singleton(topicInput))
                .topicOut(Collections.singleton(topicOutput))
                .beforeFilter(new Predicate<String, JsonNode>() {
                    @Override
                    public boolean test(String key, JsonNode jsonNode) {
                        return key.equals("keyValid");
                    }
                })
                .beforeKeyValueMapper(new KeyValueMapper<String, JsonNode, KeyValue<String, JsonNode>>() {
                    @Override
                    public KeyValue<String, JsonNode> apply(String key, JsonNode jsonNode) {
                        String newData = JSONUtils.getInstance().at(jsonNode, "myData").asText() + "-changed";
                        JSONUtils.getInstance().put(jsonNode, "myData", JsonNodeFactory.instance.textNode(newData));
                        return new KeyValue(key, jsonNode);
                    }
                })
                .afterFilter(new Predicate<String, JsonNode>() {
                    @Override
                    public boolean test(String key, JsonNode jsonNode) {
                        return !JSONUtils.getInstance().at(jsonNode, "myData").asText().startsWith("nothing");
                    }
                })
                .build();

        streamApi.createStream(request);

        driver = getDriver();
        //GenerateData
        List<KeyValue<String, JsonNode>> listData = new ArrayList<>();
        listData.add(new KeyValue("keyValid", JSONUtils.getInstance().toJsonNode(new DataExample("gnii", "1"))));
        listData.add(new KeyValue("keyValid", JSONUtils.getInstance().toJsonNode(new DataExample("nothing", "1"))));
        listData.add(new KeyValue("keyValid", JSONUtils.getInstance().toJsonNode(new DataExample("gnaa", "1"))));
        listData.add(new KeyValue("keyNotValid", JSONUtils.getInstance().toJsonNode(new DataExample("sdsdsdsd", "1"))));
        listData.add(new KeyValue("keyValid", JSONUtils.getInstance().toJsonNode(new DataExample("nothing", "1"))));
        listData.add(new KeyValue("keyValid", JSONUtils.getInstance().toJsonNode(new DataExample("gnee", "1"))));
        ConsumerRecordFactory<String, JsonNode> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new JsonNodeSerialializer());
        driver.pipeInput(recordFactory.create(topicInput, listData));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer), "keyValid", JSONUtils.getInstance().toJsonNode(new DataExample("gnii-changed", "1")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer), "keyValid", JSONUtils.getInstance().toJsonNode(new DataExample("gnaa-changed", "1")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer), "keyValid", JSONUtils.getInstance().toJsonNode(new DataExample("gnee-changed", "1")));
        Assert.assertNull(driver.readOutput(topicOutput, stringDeserializer, stringDeserializer));
    }


}
