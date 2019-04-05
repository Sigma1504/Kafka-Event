package com.accor.tech;

import com.accor.tech.api.StreamingRequest;
import com.accor.tech.serdes.GenericSerdes;
import com.accor.tech.serdes.JsonNodeSerialializer;
import com.accor.tech.utils.JSONUtils;
import com.accor.tech.utilstest.DataExample;
import com.accor.tech.utilstest.UtilsTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.common.serialization.Serdes;
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

public class StreamBuilderTest extends AbstractKafkaEventDrivenTest {
    
    @Test
    public void testStreamBuilder() {
        String topicInput = "test-in-topic";
        String topicOutput = "test-out-topic";

        ConsumerRecordFactory<String, JsonNode> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new JsonNodeSerialializer());

        final StreamingRequest request = StreamingRequest.builder()
                .streamName("LeNomduStream")
                .topicsIn(Collections.singleton(topicInput))
                .topicOut(Collections.singleton(topicOutput))
                .keyOutSerde(Serdes.String())
                .valueOutSerde(Serdes.String())
                .beforeFilter((k, v) -> k.equals("keyValid"))
                .beforeKeyValueMapper(new KeyValueMapper<String, JsonNode, KeyValue<String, String>>() {
                    @Override
                    public KeyValue<String, String> apply(String key, JsonNode value) {
                        String oldData = JSONUtils.getInstance().at(value, "myData").asText();
                        return new KeyValue(key, oldData + "-changed");
                    }
                })
                .afterFilter(new Predicate<String, String>() {
                    @Override
                    public boolean test(String key, String value) {
                        return !value.startsWith("nothing");
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

        driver.pipeInput(recordFactory.create(topicInput, listData));

        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, stringDeserializer), "keyValid", "gnii-changed");
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, stringDeserializer), "keyValid", "gnaa-changed");
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, stringDeserializer), "keyValid", "gnee-changed");
        Assert.assertNull(driver.readOutput(topicOutput, stringDeserializer, stringDeserializer));
        UtilsTest.closeDriver(driver);
    }

    @Test
    public void testStreamsOriginal() {
        String streamName = "streamNameTest";
        String topicInput = "topicInputTest";
        String topicOutput = "topicOutTest";

        final StreamingRequest request = StreamingRequest.builder()
                .streamName(streamName)
                .topicsIn(Collections.singleton(topicInput))
                .topicOut(Collections.singleton(topicOutput))
                .beforeFilter((k, v) -> k.equals("keyValid"))
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
                .keyOutSerde(Serdes.String())
                .valueOutSerde(GenericSerdes.jsonNodeSerde())
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
