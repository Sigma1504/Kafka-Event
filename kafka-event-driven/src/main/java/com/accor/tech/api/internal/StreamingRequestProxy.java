package com.accor.tech.api.internal;

import com.accor.tech.admin.config.KafkaBrokerConfig;
import com.accor.tech.api.StreamingRequest;
import com.accor.tech.api.internal.function.*;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;

import java.util.Collection;

@Getter
public class StreamingRequestProxy {

    private String streamName;
    private Collection<String> topicsIn;
    private Serde keyInSerde;
    private Serde valueInSerde;
    private PredicateProxy beforeFilter;
    private KeyValueMapperProxy beforeKeyValueMapper;
    private AggregateProxy aggregateProxy;
    private PredicateProxy afterFilter;
    private KeyValueMapperProxy afterKeyValueMapper;
    private ProcessorProxy processor;
    private Collection<String> topicOut;
    private Serde keyOutSerde;
    private Serde valueOutSerde;
    private KafkaBrokerConfig brokerConfig;

    public StreamingRequestProxy(StreamingRequest request) {
        streamName = request.getStreamName();
        topicsIn = request.getTopicsIn();
        keyInSerde = request.getKeyInSerde();
        valueInSerde = request.getValueInSerde();
        beforeFilter = request.getBeforeFilter() != null ?
                new PredicateProxy(streamName, "beforeFilter", request.getBeforeFilter()) : null;
        beforeKeyValueMapper = request.getBeforeKeyValueMapper() != null ?
                new KeyValueMapperProxy(streamName, "beforeKeyValueMapper", request.getBeforeKeyValueMapper()) : null;
        aggregateProxy = request.getAggregator() != null ?
                new AggregateProxy(request.getWindows(),
                        new InitializerProxy(streamName, request.getInitializer())
                        , new AggregatorProxy(streamName, request.getAggregator())
                        , request.getMaterialized()) : null;
        afterFilter = request.getAfterFilter() != null ?
                new PredicateProxy(streamName, "afterFilter", request.getAfterFilter()) : null;
        afterKeyValueMapper = request.getAfterKeyValueMapper() != null ?
                new KeyValueMapperProxy(streamName, "afterKeyValueMapper", request.getAfterKeyValueMapper()) : null;
        processor = request.getProcessor() != null ? new ProcessorProxy(streamName, "processor", request.getProcessor()) : null;
        topicOut = request.getTopicOut();
        keyOutSerde = request.getKeyOutSerde();
        valueOutSerde = request.getValueOutSerde();
        brokerConfig = request.getBrokerConfig();
    }
}
