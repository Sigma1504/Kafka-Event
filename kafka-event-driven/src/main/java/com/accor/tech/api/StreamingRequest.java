package com.accor.tech.api;

import com.accor.tech.admin.config.KafkaBrokerConfig;
import com.accor.tech.serdes.GenericSerdes;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;

import java.util.Collection;

@Getter
@Builder
public class StreamingRequest {

    @NonNull
    private String streamName;
    @NonNull
    private Collection<String> topicsIn;
    @Builder.Default
    private Serde keyInSerde = Serdes.String();
    @Builder.Default
    private Serde valueInSerde = GenericSerdes.jsonNodeSerde();
    private Predicate beforeFilter;
    private KeyValueMapper beforeKeyValueMapper;
    private Windows windows;
    private Initializer initializer;
    private Aggregator aggregator;
    private Materialized materialized;
    private Predicate afterFilter;
    private KeyValueMapper afterKeyValueMapper;
    private Collection<String> topicOut;
    @Builder.Default
    private Serde keyOutSerde = Serdes.String();
    @Builder.Default
    private Serde valueOutSerde = GenericSerdes.jsonNodeSerde();
    private Processor processor;
    private KafkaBrokerConfig brokerConfig;

}
