package com.accor.tech.listener;

import com.accor.tech.admin.AdminKafkaTools;
import com.accor.tech.api.StreamApi;
import com.accor.tech.api.internal.StreamExecutor;
import com.accor.tech.config.ListenerConfig;
import com.accor.tech.domain.EventActionStream;
import com.accor.tech.processor.ActionProcessorAccor;
import com.accor.tech.serdes.GenericDeserializer;
import com.accor.tech.serdes.GenericSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.UUID;

@Component
@Lazy(value = false)
@Slf4j
public class ListenerActionService {


    public final static String TOPIC_ACTION_ACCOR = "action-accor-stream";
    public final static String STORE_ACTION_ACCOR = "action-accor-store";
    public final static String TECHNICAL_STREAM_NAME_ACCOR = "technical-action-accor-stream";
    private final AdminKafkaTools adminKafkaTools;
    private final StreamExecutor streamExecutor;
    private final StreamApi streamApi;
    private final ListenerConfig listenerConfig;

    public ListenerActionService(AdminKafkaTools adminKafkaTools, StreamExecutor streamExecutor, StreamApi streamApi, ListenerConfig listenerConfig) {
        this.adminKafkaTools = adminKafkaTools;
        this.streamExecutor = streamExecutor;
        this.streamApi = streamApi;
        this.listenerConfig = listenerConfig;
    }

    @PostConstruct
    public void init() {
        // Verify topic exists ...
        adminKafkaTools.createTopic(TOPIC_ACTION_ACCOR, TopicConfig.CLEANUP_POLICY_COMPACT);
        //launch listener
        listener();
    }

    private void listener() {
        StoreBuilder<KeyValueStore<String, EventActionStream>> storeActionAccor = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_ACTION_ACCOR),
                Serdes.String(),
                GenericSerdes.eventActionStreamSerde())
                .withLoggingDisabled();
        Topology topology = new Topology();
        topology.addSource("SOURCE-TOPIC-ACTION-ACCOR", new StringDeserializer(), new GenericDeserializer(EventActionStream.class), TOPIC_ACTION_ACCOR)
                .addProcessor("ActionProcessorAccor", () -> new ActionProcessorAccor(streamExecutor, listenerConfig), "SOURCE-TOPIC-ACTION-ACCOR")
                .addStateStore(storeActionAccor, "ActionProcessorAccor");
        String applicationId = TECHNICAL_STREAM_NAME_ACCOR + UUID.randomUUID().toString();
        streamApi.createAndStartStream(topology, applicationId);
    }

}
