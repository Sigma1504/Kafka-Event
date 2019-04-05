package com.accor.tech.listener;

import com.accor.tech.api.internal.StreamExecutor;
import com.accor.tech.config.ListenerConfig;
import com.accor.tech.domain.EventActionStream;
import com.accor.tech.domain.KafkaStreamContainer;
import com.accor.tech.domain.TypeStatus;
import com.accor.tech.service.MetricService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.UUID;

@Slf4j
@Component
@Lazy(false)
public class SenderStatusService {

    private final StreamExecutor streamExecutor;
    private final ProducerEventService producerEventService;
    private final ListenerConfig listenerConfig;
    private final MetricService metricService;

    public SenderStatusService(StreamExecutor streamExecutor, ProducerEventService producerActionService, ListenerConfig listenerConfig, MetricService metricService) {
        this.streamExecutor = streamExecutor;
        this.producerEventService = producerActionService;
        this.listenerConfig = listenerConfig;
        this.metricService = metricService;
    }

    @Scheduled(initialDelay = 20 * 1000, fixedRate = 20 * 1000)
    public void senderStatus() {
        streamExecutor.getStreamsContainerMap().values().stream()
                .forEach(container -> buildAndSend(container));
    }

    private void buildAndSend(KafkaStreamContainer container) {
        if (container != null) {
            log.debug("Send status Kafkastream {}", container);
            producerEventService.produceEventActionStream(ProducerEventService.TOPIC_STATUS_ACCOR, EventActionStream.builder()
                    .streamName(container.getStreamName())
                    .instanceName(buildInstanceName())
                    .dateGeneration(new Date())
                    .eventStatus(TypeStatus.LIVE)
                    .status(container.getStreamInfo().getStreamStatus().name())
                    .mapLabels(listenerConfig.getMapLabels())
                    .mapMetrics(metricService.generateMetrics(container))
                    .build());
        }
    }

    private String buildInstanceName() {
        if (listenerConfig.getInstanceName() != null && !listenerConfig.getInstanceName().equals("")) {
            return listenerConfig.getInstanceName();
        } else {
            return "instance-name-not-referenced-" + UUID.randomUUID().toString();
        }
    }

}
