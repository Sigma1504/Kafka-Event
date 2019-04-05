package com.accor.tech;


import com.accor.tech.serdes.GenericSerdes;
import com.accor.tech.serdes.JsonNodeDeserializer;
import com.accor.tech.serdes.JsonNodeSerialializer;
import com.accor.tech.utils.JSONUtils;
import com.accor.tech.utilstest.DataExample;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.util.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class AggregateProcessorTest extends AbstractKafkaEventDrivenTest {


    private void testiAggragateFunction(String topicOutput, TopologyTestDriver driver) {
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
    }

    private List<KeyValue<String, JsonNode>> getListDataForAgg() {
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
        return listData;
    }

    @Test
    public void testProcessorApiAggregation() {
        String topicIn = "toeeeee";
        String topicOutput = "topic-out";
        ConsumerRecordFactory<String, JsonNode> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new JsonNodeSerialializer());
        StoreBuilder<KeyValueStore<String, JsonNode>> aggStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("AggStore"),
                Serdes.String(),
                GenericSerdes.jsonNodeSerde())
                .withLoggingDisabled();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ProcessorAggTetst");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Topology topology = new Topology();
        topology.addSource("sources", new StringDeserializer(), new JsonNodeDeserializer(), topicIn);
        topology.addProcessor("Aggprocessor", () -> new AggProcess(), "sources");
        topology.addStateStore(aggStore, "Aggprocessor");
        topology.addSink("out", topicOutput, new StringSerializer(), new JsonNodeSerialializer(), "Aggprocessor");

        streamApi.createAndStartStream(topology, "testTopology");
        driver = new TopologyTestDriver(topology, props);
        // GenerateData
        List<KeyValue<String, JsonNode>> listData = getListDataForAgg();

        driver.pipeInput(recordFactory.create(topicIn, listData));
        testiAggragateFunction(topicOutput, driver);
        Assert.assertNull(driver.readOutput(topicOutput, stringDeserializer, jsonNodeDeserializer));

    }


    private class AggProcess extends AbstractProcessor<String, JsonNode> {
        private KeyValueStore<String, JsonNode> valueStore;

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            valueStore = (KeyValueStore) context.getStateStore("AggStore");
        }

        @Override
        public void process(String key, JsonNode value) {

            if (!StringUtils.isEmpty(key)) {

                //filter
                if ("C".equals(JSONUtils.getInstance().at(value, "action").asText()) || "P".equals(JSONUtils.getInstance().at(value, "action").asText())) {

                    JsonNode valueStored = valueStore.get(key);
                    if (valueStored == null) {
                        //aggregator init
                        valueStored = JSONUtils.getInstance().toJsonNode(new DataExample("DefKey", "NO_Custo", "NO_P", "AGG"));
                    }
                    //aggregate.apply
                    if ("C".equals(JSONUtils.getInstance().at(value, "action").asText())) {

                        JSONUtils.getInstance().put(valueStored, "custo", JsonNodeFactory.instance.textNode(JSONUtils.getInstance().at(value, "custo").asText()));
                    } else {
                        JSONUtils.getInstance().put(valueStored, "pay", JsonNodeFactory.instance.textNode(JSONUtils.getInstance().at(value, "pay").asText()));
                    }
                    JSONUtils.getInstance().put(valueStored, "myData", JsonNodeFactory.instance.textNode(JSONUtils.getInstance().at(value, "myData").asText()));

                    valueStore.put(key, valueStored);

                    context().forward(key, valueStored);
                }
            }

        }
    }
}


