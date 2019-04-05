package com.accor.tech.api.internal;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.accor.tech.api.internal.HeaderUtils.*;

public class ConsumerProxy<K, V> implements Consumer<K, V> {

    private String streamName;
    private Consumer<K, V> consumerReference;

    public ConsumerProxy(Consumer<K, V> consumerReference, String streamName) {
        this.consumerReference = consumerReference;
        this.streamName = streamName;
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        return poll(Duration.ofMillis(timeout));
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration duration) {
        ConsumerRecords<K, V> records = consumerReference.poll(duration);
        if (records.isEmpty()) return records;
        for (TopicPartition partition : records.partitions()) {
            records.records(partition).stream()
                    .forEach(record -> addHeaderAccor(record));
        }
        return records;
    }

    private void addHeaderAccor(ConsumerRecord<K, V> record) {
        addHeaderIfNotExist(record.headers(), streamName, record.topic());
        record.headers().add(new RecordHeader(HEADER_TOPIC_IN, record.topic().getBytes()));
        record.headers().add(new RecordHeader(HEADER_TIMESTAMP_CONSUMER_CORRELATION_ID, String.valueOf(System.currentTimeMillis()).getBytes()));
    }

    @Override
    public Set<TopicPartition> assignment() {
        return consumerReference.assignment();
    }

    @Override
    public Set<String> subscription() {
        return consumerReference.subscription();
    }

    @Override
    public void subscribe(Collection<String> collection) {
        consumerReference.subscribe(collection);
    }

    @Override
    public void subscribe(Collection<String> collection, ConsumerRebalanceListener consumerRebalanceListener) {
        consumerReference.subscribe(collection, consumerRebalanceListener);
    }

    @Override
    public void assign(Collection<TopicPartition> collection) {
        consumerReference.assign(collection);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener consumerRebalanceListener) {
        consumerReference.subscribe(pattern, consumerRebalanceListener);
    }

    @Override
    public void subscribe(Pattern pattern) {
        consumerReference.subscribe(pattern);
    }

    @Override
    public void unsubscribe() {
        consumerReference.unsubscribe();
    }


    @Override
    public void commitSync() {
        consumerReference.commitSync();
    }

    @Override
    public void commitSync(Duration duration) {
        consumerReference.commitSync(duration);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> map) {
        consumerReference.commitSync(map);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> map, Duration duration) {
        consumerReference.commitSync(map, duration);
    }

    @Override
    public void commitAsync() {
        consumerReference.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback offsetCommitCallback) {
        consumerReference.commitAsync(offsetCommitCallback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> map, OffsetCommitCallback offsetCommitCallback) {
        consumerReference.commitAsync(map, offsetCommitCallback);
    }

    @Override
    public void seek(TopicPartition topicPartition, long l) {
        consumerReference.seek(topicPartition, l);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> collection) {
        consumerReference.seekToBeginning(collection);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> collection) {
        consumerReference.seekToEnd(collection);
    }

    @Override
    public long position(TopicPartition topicPartition) {
        return consumerReference.position(topicPartition);
    }

    @Override
    public long position(TopicPartition topicPartition, Duration duration) {
        return consumerReference.position(topicPartition, duration);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition topicPartition) {
        return consumerReference.committed(topicPartition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition topicPartition, Duration duration) {
        return consumerReference.committed(topicPartition, duration);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return consumerReference.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        return consumerReference.partitionsFor(s);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s, Duration duration) {
        return consumerReference.partitionsFor(s, duration);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return consumerReference.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration duration) {
        return consumerReference.listTopics(duration);
    }

    @Override
    public Set<TopicPartition> paused() {
        return consumerReference.paused();
    }

    @Override
    public void pause(Collection<TopicPartition> collection) {
        consumerReference.paused();
    }

    @Override
    public void resume(Collection<TopicPartition> collection) {
        consumerReference.resume(collection);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> map) {
        return consumerReference.offsetsForTimes(map);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> map, Duration duration) {
        return consumerReference.offsetsForTimes(map, duration);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> collection) {
        return consumerReference.beginningOffsets(collection);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> collection, Duration duration) {
        return consumerReference.beginningOffsets(collection, duration);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> collection) {
        return consumerReference.endOffsets(collection);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> collection, Duration duration) {
        return consumerReference.endOffsets(collection, duration);
    }

    @Override
    public void close() {
        consumerReference.close();
    }

    @Override
    public void close(long l, TimeUnit timeUnit) {
        consumerReference.close(l, timeUnit);
    }

    @Override
    public void close(Duration duration) {
        consumerReference.close(duration);
    }

    @Override
    public void wakeup() {
        consumerReference.wakeup();
    }
}
