package com.accor.tech;

import com.accor.tech.api.StreamingRequest;
import com.accor.tech.serdes.GenericSerdes;
import com.accor.tech.serdes.JsonNodeSerialializer;
import com.accor.tech.utils.JSONUtils;
import com.accor.tech.utilstest.DataExample;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class AggregateAPiTest extends AbstractKafkaEventDrivenTest {

   
    @Test
    public void testAggregateAPI() {

        String topicIn = "toeeeee";
        String topicOutput = "topic-out";
        ConsumerRecordFactory<String, JsonNode> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new JsonNodeSerialializer());

        final StreamingRequest request = StreamingRequest.builder()
                .streamName("TestAggNew")
                .topicsIn(Collections.singleton(topicIn))
                .beforeFilter(new Predicate<String, JsonNode>() {
                    @Override
                    public boolean test(String key, JsonNode v) {
                        return "C".equals(JSONUtils.getInstance().at(v, "action").asText()) || "P".equals(JSONUtils.getInstance().at(v, "action").asText());
                    }
                })
                .initializer(() -> JSONUtils.getInstance().toJsonNode(new DataExample("DefKey", "NO_Custo", "NO_P", "AGG")))
                .aggregator(new Aggregator<String, JsonNode, JsonNode>() {
                    @Override
                    public JsonNode apply(String key, JsonNode newValue, JsonNode oldValue) {
                        log.error(key + "|" + JSONUtils.getInstance().at(oldValue, "myData").asText() + "-" + JSONUtils.getInstance().at(oldValue, "custo").asText() + "-" + JSONUtils.getInstance().at(oldValue, "pay").asText() + "-" + JSONUtils.getInstance().at(oldValue, "action").asText());
                        log.error("\t|" + JSONUtils.getInstance().at(newValue, "myData").asText() + "-" + JSONUtils.getInstance().at(newValue, "custo").asText() + "-" + JSONUtils.getInstance().at(newValue, "pay").asText() + "-" + JSONUtils.getInstance().at(newValue, "action").asText());

                        if ("C".equals(JSONUtils.getInstance().at(newValue, "action").asText())) {

                            JSONUtils.getInstance().put(oldValue, "custo", JsonNodeFactory.instance.textNode(JSONUtils.getInstance().at(newValue, "custo").asText()));
                        } else {
                            JSONUtils.getInstance().put(oldValue, "pay", JsonNodeFactory.instance.textNode(JSONUtils.getInstance().at(newValue, "pay").asText()));
                        }
                        JSONUtils.getInstance().put(oldValue, "myData", JsonNodeFactory.instance.textNode(JSONUtils.getInstance().at(newValue, "myData").asText()));

                        return oldValue;
                    }
                })
                .materialized(Materialized.with(Serdes.String(), GenericSerdes.jsonNodeSerde()))
                .afterFilter((k, v) -> true)
                .topicOut(Collections.singleton(topicOutput))
                .build();

        streamApi.createStream(request);

        driver = getDriver();

//        GenerateData
        List<KeyValue<String, JsonNode>> listData = new LinkedList<>();
        listData.add(new KeyValue("k1", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "1", "", "X"))));
        listData.add(new KeyValue("k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "custo", "CB", "C"))));
        listData.add(new KeyValue("k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "nv_custo2", "", "C"))));
        listData.add(new KeyValue("k3", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "toto", "", "C"))));
        listData.add(new KeyValue("k1", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "", "CB", "P"))));
        listData.add(new KeyValue("k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "", "VISA", "P"))));
        listData.add(new KeyValue("k3", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "titi", "", "C"))));
        listData.add(new KeyValue("k3", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "toto", "", "X"))));
        listData.add(new KeyValue("k1", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "", "CB", "X"))));
        listData.add(new KeyValue("k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "3", "VISA", "X"))));
        listData.add(new KeyValue("k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "end_custo4", "", "C"))));
        listData.add(new KeyValue("k1", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "custo1", "", "C"))));
        listData.add(new KeyValue("k3", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "toto_nv", "", "C"))));
        listData.add(new KeyValue("k1", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "1", "", "C"))));
        listData.add(new KeyValue("k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "15", "VISA1", "P"))));
        listData.add(new KeyValue("k3", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "to", "CB", "P"))));

        driver.pipeInput(recordFactory.create(topicIn, listData));

        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "custo", "NO_P", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "nv_custo2", "NO_P", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k3", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "toto", "NO_P", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k1", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "NO_Custo", "CB", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "nv_custo2", "VISA", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k3", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "titi", "NO_P", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "end_custo4", "VISA", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k1", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "custo1", "CB", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k3", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "toto_nv", "NO_P", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k1", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "1", "CB", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "end_custo4", "VISA1", "AGG")));
        OutputVerifier.compareKeyValue(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer),
                "k3", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "toto_nv", "CB", "AGG")));

        Assert.assertNull(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer));
    }


}
