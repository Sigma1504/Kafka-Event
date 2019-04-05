package com.accor.tech.repository;

import com.accor.tech.admin.AdminKafkaTools;
import com.accor.tech.admin.config.KafkaBrokerConfig;
import com.accor.tech.admin.config.KafkaUtils;
import com.accor.tech.serdes.JsonNodeSerialializer;
import com.accor.tech.utils.JSONUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Streams;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class AbstractKafkaRepository<V> {

    private final Producer<String, JsonNode> producer;
    @Getter
    private final ReadOnlyKeyValueStore<String, V> keyValueStore;
    private final KafkaBrokerConfig kafkaBrokerConfig;
    private final AdminKafkaTools adminKafkaTools;
    @Getter
    private final String repositoryName;
    private final String topic;

    public AbstractKafkaRepository(String topic, String repositoryName, Serde<V> valueSerde, AdminKafkaTools adminKafkaTools, KafkaBrokerConfig kafkaBrokerConfig) {
        this.adminKafkaTools = adminKafkaTools;
        this.topic = topic;
        this.repositoryName = repositoryName;
        this.kafkaBrokerConfig = kafkaBrokerConfig;
        this.producer = KafkaUtils.kafkaProducer(this.kafkaBrokerConfig, StringSerializer.class, JsonNodeSerialializer.class);
        StreamsBuilder builder = new StreamsBuilder();
        adminKafkaTools.createTopic(topic, TopicConfig.CLEANUP_POLICY_COMPACT);
        final GlobalKTable<String, V>
                globalKTableInput =
                builder.globalTable(topic, Materialized.<String, V, KeyValueStore<Bytes, byte[]>>as(repositoryName)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(valueSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), KafkaUtils.getKafkaStreamsConfigs(this.kafkaBrokerConfig, repositoryName + "-stream"));
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        kafkaStreams.start();
        producer.flush();
        this.keyValueStore = kafkaStreams.store(repositoryName, QueryableStoreTypes.keyValueStore());
    }

    public List<V> findAll() {
        return Streams.stream(keyValueStore.all())
                .filter(entry -> entry.value != null)
                .map(entry -> entry.value)
                .collect(Collectors.toList());
    }

    public V findByKey(String key) {
        return keyValueStore.get(key);
    }

    public void deleteAll() {
        keyValueStore.all().forEachRemaining(entry -> deleteByKey(entry.key));
    }

    public void deleteByKey(String key) {
        produce(key, null);
    }

    public void saveOrUpdate(String key, V entity) {
        produce(key, entity);
    }

    private void produce(String key, V value) {
        if (key != null) {
            producer.send(new ProducerRecord<>(topic, key, value != null ? JSONUtils.getInstance().toJsonNode(value) : null));
            producer.flush();
        }
    }


}
