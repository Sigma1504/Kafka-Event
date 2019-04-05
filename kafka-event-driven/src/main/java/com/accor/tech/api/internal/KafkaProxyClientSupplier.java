package com.accor.tech.api.internal;

import com.accor.tech.config.ListenerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;

import java.util.Map;

public class KafkaProxyClientSupplier implements KafkaClientSupplier {

    private String streamName;
    private ListenerConfig listenerConfig;

    public KafkaProxyClientSupplier(String streamName, ListenerConfig listenerConfig) {
        this.listenerConfig = listenerConfig;
        this.streamName = streamName;
    }

    @Override
    public AdminClient getAdminClient(Map<String, Object> mapConfig) {
        return AdminClient.create(mapConfig);
    }

    @Override
    public Producer<byte[], byte[]> getProducer(Map<String, Object> mapConfig) {
        mapConfig.put("key.serializer", ByteArraySerializer.class);
        mapConfig.put("value.serializer", ByteArraySerializer.class);
        Producer<byte[], byte[]> producer = new KafkaProducer<>(mapConfig);
        return new ProducerProxy<>(producer, streamName, listenerConfig);
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(Map<String, Object> mapConfig) {
        mapConfig.put("key.deserializer", ByteArrayDeserializer.class);
        mapConfig.put("value.deserializer", ByteArrayDeserializer.class);
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(mapConfig);
        return new ConsumerProxy<>(consumer, streamName);
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> mapConfig) {
        return getConsumer(mapConfig);
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> mapConfig) {
        return getConsumer(mapConfig);
    }
}
