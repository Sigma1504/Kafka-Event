package com.accor.tech.adminevent.repository;

import com.accor.tech.admin.AdminKafkaTools;
import com.accor.tech.admin.config.KafkaBrokerConfig;
import com.accor.tech.domain.EventActionStream;
import com.accor.tech.domain.TypeStatus;
import com.accor.tech.repository.AbstractKafkaRepository;
import com.accor.tech.serdes.GenericSerdes;
import com.google.common.collect.Streams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

import static com.accor.tech.adminevent.listener.ListenerStatusService.STORE_STATUS_ACCOR;
import static com.accor.tech.adminevent.listener.ListenerStatusService.TOPIC_STATUS_ACCOR;

@Slf4j
@Component
public class StatusRepository extends AbstractKafkaRepository<EventActionStream> {

    public StatusRepository(KafkaBrokerConfig kafkaBrokerConfig, AdminKafkaTools adminKafkaTools) {
        super(TOPIC_STATUS_ACCOR, STORE_STATUS_ACCOR, GenericSerdes.eventActionStreamSerde(), adminKafkaTools, kafkaBrokerConfig);
    }

    public List<EventActionStream> findByStatus(TypeStatus typeAction) {
        return Streams.stream(getKeyValueStore().all())
                .map(entry -> entry.value)
                .filter(value -> value != null)
                .filter(value -> value.getEventStatus() == typeAction)
                .collect(Collectors.toList());
    }

    public List<EventActionStream> findByInstanceName(String instanceName) {
        return Streams.stream(getKeyValueStore().all())
                .map(entry -> entry.value)
                .filter(value -> value != null)
                .filter(value -> value.getInstanceName().startsWith(instanceName))
                .collect(Collectors.toList());
    }



}
