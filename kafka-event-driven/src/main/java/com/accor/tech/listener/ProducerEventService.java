package com.accor.tech.listener;

import com.accor.tech.admin.config.KafkaBrokerConfig;
import com.accor.tech.admin.config.KafkaUtils;
import com.accor.tech.domain.EventActionStream;
import com.accor.tech.domain.TypeAction;
import com.accor.tech.domain.TypeStatus;
import com.accor.tech.serdes.EventActionStreamSerialializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;

@Component
@Slf4j
public class ProducerEventService {

    public final static String TOPIC_STATUS_ACCOR = "status-accor-stream";
    private final Producer<String, EventActionStream> producer;
    private final KafkaBrokerConfig kafkaBrokerConfig;

    public ProducerEventService(KafkaBrokerConfig kafkaBrokerConfig) {
        this.kafkaBrokerConfig = kafkaBrokerConfig;
        this.producer = KafkaUtils.kafkaProducer(this.kafkaBrokerConfig, StringSerializer.class, EventActionStreamSerialializer.class);
    }

    public void produceAction(TypeAction typeAction, String streamName, String instanceName, HashMap<String, String> mapLabels) {
        sendToKafka(ListenerActionService.TOPIC_ACTION_ACCOR, EventActionStream.builder()
                .streamName(streamName)
                .typeAction(typeAction)
                .dateGeneration(new Date())
                .mapLabels(mapLabels)
                .instanceName(instanceName)
                .build());
    }

    public void produceStatus(TypeStatus eventStatus, String streamName, String instanceName, HashMap<String, String> mapLabels) {
        sendToKafka(TOPIC_STATUS_ACCOR, EventActionStream.builder()
                .streamName(streamName)
                .eventStatus(eventStatus)
                .dateGeneration(new Date())
                .mapLabels(mapLabels)
                .instanceName(instanceName)
                .build());
    }

    public void produceEventActionStream(String topic, EventActionStream eventActionStream) {
        sendToKafka(topic, eventActionStream);
    }

    private void sendToKafka(String topic, EventActionStream eventActionStream) {
        try {
            log.debug("Sending EventActionStream {}", eventActionStream);
            producer.send(new ProducerRecord(topic, eventActionStream.getStreamName(), eventActionStream));
            producer.flush();
        } catch (Exception e) {
            log.error("Error sending to Kafka during generation ", e);
        }
    }

}
