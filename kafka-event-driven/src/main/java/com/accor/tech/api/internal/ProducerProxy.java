package com.accor.tech.api.internal;

import com.accor.tech.config.ListenerConfig;
import com.accor.tech.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.Header;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.accor.tech.api.internal.HeaderUtils.*;

@Slf4j
public class ProducerProxy<K, V> implements Producer<K, V> {

    private String streamName;
    private Producer<K, V> producerReference;
    private ListenerConfig listenerConfig;

    public ProducerProxy(Producer<K, V> producerReference, String streamName, ListenerConfig listenerConfig) {
        this.producerReference = producerReference;
        this.streamName = streamName;
        this.listenerConfig = listenerConfig;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
        return producerReference.send(producerRecord, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
        addLog(producerRecord);
        addHeaderAccor(producerRecord);
        return producerReference.send(producerRecord);
    }

    private void addLog(ProducerRecord<K, V> producerRecord) {
        if (listenerConfig.getLogAllProduce()) {
            produceLog(producerRecord);
        } else if (!listenerConfig.getMapTopicProduceLog().isEmpty() && listenerConfig.getMapTopicProduceLog().get(producerRecord.topic()) != null) {
            produceLog(producerRecord);
        }
    }

    private void produceLog(ProducerRecord<K, V> producerRecord) {
        String headerCorrelationId = extractHeader(producerRecord.headers(), HEADER_CORRELATION_ID);
        String headerStreamName = extractHeader(producerRecord.headers(), HEADER_STREAM_NAME);
        String headerTopicOriginal = extractHeader(producerRecord.headers(), HEADER_TOPIC_ORIGINAL);
        String headerTopicIn = extractHeader(producerRecord.headers(), HEADER_TOPIC_IN);
        String headerTimestampTopicIn = extractHeader(producerRecord.headers(), HEADER_TIMESTAMP_CONSUMER_CORRELATION_ID);
        DateFormat df = new SimpleDateFormat("dd:MM:yy:HH:mm:ss");
        String dateResult = NO_VALUE;
        if (!headerTimestampTopicIn.equals(NO_VALUE)) {
            dateResult = df.format(new Date(Long.parseLong(headerTimestampTopicIn)));
        }
        log.info("Stream " + headerStreamName +
                " Produce data from " + headerTopicIn +
                " with timestamp " + dateResult +
                " to topic " + producerRecord.topic() +
                " with the Key " + producerRecord.key().toString() +
                " with correlation id " + headerCorrelationId +
                " topic original " + headerTopicOriginal);
    }

    private void addHeaderAccor(ProducerRecord<K, V> record) {
        Optional<Header> header = Arrays.asList(record.headers().toArray()).stream()
                .filter(itemHeader -> itemHeader.key().equals(HEADER_TIMESTAMP_CONSUMER_CORRELATION_ID))
                .findFirst();
        if (header.isPresent()) {
            Long timestampConsumer = Long.parseLong(new String(header.get().value()));
            metricTopicToTopic(timestampConsumer, record, HEADER_TOPIC_ORIGINAL);
            metricTopicToTopic(timestampConsumer, record, HEADER_TOPIC_IN);
        }
        //remove
        record.headers().remove(HEADER_TIMESTAMP_CONSUMER_CORRELATION_ID);
        record.headers().remove(HEADER_TOPIC_IN);
    }

    private void metricTopicToTopic(Long timestampConsumer, ProducerRecord<K, V> record, String headerTopicIn) {
        String topicIn = extractHeader(record.headers(), headerTopicIn);
        if (topicIn != null && !topicIn.equals(NO_VALUE)) {
            MetricsUtils.timerMetricTopicToTopic(streamName, topicIn, record.topic(), System.currentTimeMillis() - timestampConsumer);
        }
    }

    @Override
    public void initTransactions() {
        producerReference.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        producerReference.beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, String s) throws ProducerFencedException {
        producerReference.sendOffsetsToTransaction(map, s);
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        producerReference.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        producerReference.abortTransaction();
    }

    @Override
    public void flush() {
        producerReference.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        return producerReference.partitionsFor(s);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producerReference.metrics();
    }

    @Override
    public void close() {
        producerReference.close();
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        producerReference.close(l, timeUnit);
    }
}
