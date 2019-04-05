package com.accor.tech.utilstest;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class UtilsTest {

    private static String TEMP_DIRECTORY = "C:/tmp/kafka-streams";

    public static Properties generatePropertiesForTest() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "filter" + UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public static TopologyTestDriver getDriver(StreamsBuilder builder) {
        Topology topology = builder.build();
        return new TopologyTestDriver(topology, UtilsTest.generatePropertiesForTest());
    }

    public static void loggerDebug(TopologyTestDriver driver, String topicOut, Deserializer keyDeserializer, Deserializer valueDeserializer) {
        boolean test = true;
        ProducerRecord record;
        while (test) {
            record = driver.readOutput(topicOut, keyDeserializer, valueDeserializer);
            if (record != null)
                log.debug("key: {} value: {}", record.key(), record.value());
            else
                test = false;
        }
    }

    public static void closeDriver(TopologyTestDriver driver) {
        String os = System.getProperty("os.name");
        if (os != null && os.toLowerCase().startsWith("win")) {
            try {
                Thread.sleep(2000);
                FileSystemUtils.deleteRecursively(new File(TEMP_DIRECTORY));
            } catch (Exception e) {
                log.error("Impossible delete file {}", e);
            }
        } else {
            driver.close();
        }
    }
}
