package com.accor.tech;

import com.accor.tech.admin.AdminKafkaTools;
import com.accor.tech.api.StreamApi;
import com.accor.tech.api.internal.KafkaStreamContainerBuilder;
import com.accor.tech.api.internal.StreamExecutor;
import com.accor.tech.config.ListenerConfig;
import com.accor.tech.listener.ProducerEventService;
import com.accor.tech.serdes.JsonNodeDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.util.reflection.FieldSetter;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
public abstract class AbstractKafkaEventDrivenTest {

    String TEMP_DIRECTORY = "C:/tmp/kafka-streams";
    ProducerEventService producerActionService;
    ListenerConfig listenerConfig;
    StreamExecutor streamExecutor;
    StreamApi streamApi;
    KafkaStreamContainerBuilder kafkaStreamContainerBuilder;
    ArgumentCaptor<StreamsBuilder> valueCaptureStreamsBuilder;
    StringDeserializer stringDeserializer = new StringDeserializer();
    JsonNodeDeserializer jsonNodeDeserializer = new JsonNodeDeserializer();
    AdminKafkaTools adminKafkaTools;
    TopologyTestDriver driver;


    @Before
    public void preProcessForMockSocle() throws Exception {
        streamExecutor = new StreamExecutor(producerActionService, listenerConfig, kafkaStreamContainerBuilder);
        kafkaStreamContainerBuilder = mock(KafkaStreamContainerBuilder.class);
        adminKafkaTools = mock(AdminKafkaTools.class);
        FieldSetter.setField(kafkaStreamContainerBuilder, KafkaStreamContainerBuilder.class.getDeclaredField("adminKafkaTools"), adminKafkaTools);
        //Call real method
        doCallRealMethod().when(kafkaStreamContainerBuilder).createKafkaStreamContainer(any());

        //Capture streambuilder pour exécuter le test avec la class de kafka test
        valueCaptureStreamsBuilder = ArgumentCaptor.forClass(StreamsBuilder.class);
        when(kafkaStreamContainerBuilder.createKafkaStream(any(), valueCaptureStreamsBuilder.capture(), any())).thenReturn(null);
        when(adminKafkaTools.existTopics(any())).thenReturn(true);
        streamApi = new StreamApi(streamExecutor, kafkaStreamContainerBuilder);
    }

    @After
    public void closeDriver() {
        String os = System.getProperty("os.name");
        if (os != null && os.toLowerCase().startsWith("win")) {
            try {
                Thread.sleep(2000);
                FileSystemUtils.deleteRecursively(new File(TEMP_DIRECTORY));
            } catch (Exception e) {
                log.error("Impossible delete file {}", e);
            }
        } else {
            this.driver.close();
        }
    }

    Properties generatePropertiesForTest() {
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


    TopologyTestDriver getDriver() {
        return getDriver(valueCaptureStreamsBuilder.getValue());
    }

    TopologyTestDriver getDriver(StreamsBuilder builder) {
        Topology topology = builder.build();
        return new TopologyTestDriver(topology, generatePropertiesForTest());
    }


    void loggerDebug(TopologyTestDriver driver, String topicOut, Deserializer keyDeserializer, Deserializer valueDeserializer) {
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


}
