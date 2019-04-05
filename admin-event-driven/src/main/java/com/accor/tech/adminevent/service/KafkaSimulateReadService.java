package com.accor.tech.adminevent.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class KafkaSimulateReadService {

    private KafkaConsumer<String, String> kafkaConsumerString(String offReset, String bootStrapServers, String groupId, String deserializer) {
        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offReset);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "20000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "5048576");
        return new KafkaConsumer<>(props);
    }

    public List<String> readKafkaRawData(String bootStrapServers, String topic, String maxRecords, String windowTime, String offReset, String deserializer) {
        KafkaConsumer kafkaConsumer;
        if (offReset.equalsIgnoreCase("last")) {
            kafkaConsumer = kafkaConsumerString("latest", bootStrapServers, "simulate-raw" + UUID.randomUUID().toString(), deserializer);
            kafkaConsumer.subscribe(Arrays.asList(topic));
            kafkaConsumer.poll(1);
            Set<TopicPartition> assignment = kafkaConsumer.assignment();
            Map<TopicPartition, Long> offsetsPerPartition = kafkaConsumer.endOffsets(assignment);
            offsetsPerPartition.entrySet().stream().forEach(e -> resetOffset(kafkaConsumer, e.getKey(), Math.max(e.getValue() - Long.valueOf(maxRecords), 0)));
        } else {
            kafkaConsumer = kafkaConsumerString(offReset, bootStrapServers, "simulate-raw" + UUID.randomUUID().toString(), deserializer);
            kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        log.info("Subscribe Topic for {} with parameter offReset {} windowTime {} maxRecords {} ", topic, offReset, windowTime, maxRecords);

        List<String> res = new ArrayList<>();
        long start = System.currentTimeMillis();
        try {
            while (checkWindow(start, Long.valueOf(windowTime), res.size(), Long.valueOf(maxRecords))) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    log.debug(record.value());
                    res.add(record.value());
                }
                kafkaConsumer.commitSync();
            }
            log.info("Number item for read Raw Data {}", res.size());
        } catch (WakeupException e) {
            // Ignore exception if closing
            throw e;
        } catch (RuntimeException re) {
            log.error("RuntimeException {}", re);
        } finally {
            kafkaConsumer.close();
        }
        return res;
    }

    private void resetOffset(KafkaConsumer kafkaConsumer, TopicPartition topicPartition, long newPosition) {
        log.error("Reseting partition position on {} partition {} to {}", topicPartition.topic(), topicPartition.partition(), newPosition);
        kafkaConsumer.seek(topicPartition, newPosition);
    }

    private Boolean checkWindow(long start, Long windowTime, long sizeList, long maxSizeItems) {
        long current = System.currentTimeMillis();
        if (current >= (start + windowTime.longValue())) {
            return false;
        }
        if (sizeList >= maxSizeItems) {
            return false;
        }
        return true;
    }
}
