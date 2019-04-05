package com.accor.tech.adminevent.listener;

import com.accor.tech.admin.AdminKafkaTools;
import com.accor.tech.adminevent.processor.StatusProcessorAccor;
import com.accor.tech.api.StreamApi;
import com.accor.tech.api.internal.StreamExecutor;
import com.accor.tech.config.ListenerConfig;
import com.accor.tech.domain.EventActionStream;
import com.accor.tech.serdes.GenericDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@Lazy(value = false)
@Slf4j
public class ListenerStatusService {

    public final static String TOPIC_STATUS_ACCOR = "status-accor-stream";
    public final static String STORE_STATUS_ACCOR = "status-accor-store";
    public final static String TECHNICAL_STREAM_NAME_ACCOR = "technical-status-accor-stream";
    private final AdminKafkaTools adminKafkaTools;
    private final StreamExecutor streamExecutor;
    private final StreamApi streamApi;
    private final ListenerConfig listenerConfig;

    public ListenerStatusService(AdminKafkaTools adminKafkaTools, StreamExecutor streamExecutor, StreamApi streamApi, ListenerConfig listenerConfig) {
        this.adminKafkaTools = adminKafkaTools;
        this.streamExecutor = streamExecutor;
        this.streamApi = streamApi;
        this.listenerConfig = listenerConfig;
    }

    @PostConstruct
    public void init() {
        // Verify topic exists ...
        adminKafkaTools.createTopic(TOPIC_STATUS_ACCOR, TopicConfig.CLEANUP_POLICY_COMPACT);
        //launch listener
        listener();
    }

    private void listener() {
        Topology topology = new Topology();
        topology.addSource("SOURCE-TOPIC-STATUS-ACCOR", new StringDeserializer(), new GenericDeserializer(EventActionStream.class), TOPIC_STATUS_ACCOR)
                .addProcessor("StatusProcessorAccor", () -> new StatusProcessorAccor(streamExecutor, listenerConfig), "SOURCE-TOPIC-STATUS-ACCOR");
        streamApi.createAndStartStream(topology, TECHNICAL_STREAM_NAME_ACCOR);

    }

}
