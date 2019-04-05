package com.accor.tech;

import com.accor.tech.serdes.GenericSerdes;
import com.accor.tech.serdes.JsonNodeSerialializer;
import com.accor.tech.utils.JSONUtils;
import com.accor.tech.utilstest.DataExample;
import com.accor.tech.utilstest.UtilsTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

@Slf4j
public class StreamWithNoFwkTest extends AbstractKafkaEventDrivenTest {
    
    @Test
    public void testStreams() {
        String topicInternal = "topicInternaltestStreams1";

        String topicOutput = "topicOuttestStreams1";

        ConsumerRecordFactory<String, JsonNode> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new JsonNodeSerialializer());

        // create KafkaStream
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> kStream = builder.stream(topicInternal, Consumed.with(Serdes.String(), GenericSerdes.jsonNodeSerde()));

        /*
         * vérif données action
         * changer la clé
         * agréger les données client et paiement
         * sortir sur le topic out
         * */
        kStream
                .filter((k, v) -> "C".equals(JSONUtils.getInstance().at(v, "action").asText()) || "P".equals(JSONUtils.getInstance().at(v, "action").asText()))
                .map((k, v) -> KeyValue.pair(JSONUtils.getInstance().at(v, "myData").asText(), v))
                .groupByKey(Grouped.with(Serdes.String(), GenericSerdes.jsonNodeSerde()))
                .aggregate(new Initializer<JsonNode>() {
                               @Override
                               public JsonNode apply() {
                                   return JSONUtils.getInstance().toJsonNode(new DataExample("DefKey", "NO_Custo", "NO_P", "AGG"));
                               }
                           },
                        new Aggregator<String, JsonNode, JsonNode>() {
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
                        },
                        Materialized.with(Serdes.String(), GenericSerdes.jsonNodeSerde()))
                .toStream()
                .to(topicOutput, Produced.with(Serdes.String(), GenericSerdes.jsonNodeSerde()));
        streamApi.createStream(builder, "testStreamBuildertestStreams1");

        driver = UtilsTest.getDriver(builder);

//        GenerateData
        List<KeyValue<String, JsonNode>> listData = new LinkedList<>();
        listData.add(new KeyValue("key1", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "1", "", "X"))));
        listData.add(new KeyValue("key2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "custo", "CB", "C"))));
        listData.add(new KeyValue("key3", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "nv_custo2", "", "C"))));
        listData.add(new KeyValue("key4", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "toto", "", "C"))));
        listData.add(new KeyValue("key5", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "", "CB", "P"))));
        listData.add(new KeyValue("key2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "", "VISA", "P"))));
        listData.add(new KeyValue("key4", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "titi", "", "C"))));
        listData.add(new KeyValue("key4", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "toto", "", "X"))));
        listData.add(new KeyValue("key5", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "", "CB", "X"))));
        listData.add(new KeyValue("key2", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "3", "VISA", "X"))));
        listData.add(new KeyValue("key6", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "end_custo4", "", "C"))));
        listData.add(new KeyValue("key3", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "custo1", "", "C"))));
        listData.add(new KeyValue("key4", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "toto_nv", "", "C"))));
        listData.add(new KeyValue("key5", JSONUtils.getInstance().toJsonNode(new DataExample("k1", "1", "", "C"))));
        listData.add(new KeyValue("key6", JSONUtils.getInstance().toJsonNode(new DataExample("k2", "15", "VISA1", "P"))));
        listData.add(new KeyValue("key4", JSONUtils.getInstance().toJsonNode(new DataExample("k3", "to", "CB", "P"))));

        driver.pipeInput(recordFactory.create(topicInternal, listData));
        //??
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
