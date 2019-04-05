package com.accor.tech.api.internal;

import com.accor.tech.admin.AdminKafkaTools;
import com.accor.tech.admin.config.KafkaAdminConfig;
import com.accor.tech.admin.config.KafkaBrokerConfig;
import com.accor.tech.admin.config.KafkaUtils;
import com.accor.tech.api.internal.function.AggregateProxy;
import com.accor.tech.api.internal.function.AggregatorProxy;
import com.accor.tech.api.internal.function.InitializerProxy;
import com.accor.tech.config.ListenerConfig;
import com.accor.tech.domain.KafkaStreamContainer;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;



@Component
public class KafkaStreamContainerBuilder {

    private final KafkaBrokerConfig kafkaBrokerConfig;
    private final AdminKafkaTools adminKafkaTools;
    private final ListenerConfig listenerConfig;

    public KafkaStreamContainerBuilder(KafkaBrokerConfig kafkaBrokerConfig, ListenerConfig listenerConfig) {
        this.kafkaBrokerConfig = kafkaBrokerConfig;
        this.adminKafkaTools = new AdminKafkaTools(kafkaBrokerConfig, new KafkaAdminConfig());
        this.listenerConfig = listenerConfig;
    }

    public KafkaStreamContainer createKafkaStreamContainer(StreamingRequestProxy request) throws TopicExistsException {

        if (!adminKafkaTools.existTopics(request.getTopicsIn())) {
            throw new TopicExistsException("Topics doesn't exist! Please used the AdminKafkaTools to create topics", new Throwable("Topics doesn't exist"));
        }
        
        StreamsBuilder builder = new StreamsBuilder();
        final String streamName = request.getStreamName();

        KStream kStream = builder.stream(request.getTopicsIn(), Consumed.with(request.getKeyInSerde(), request.getValueInSerde()));
        if (request.getBeforeFilter() != null) {
            kStream = kStream.filter(request.getBeforeFilter());
        }
        if (request.getBeforeKeyValueMapper() != null) {
            kStream = kStream.map(request.getBeforeKeyValueMapper()); 
        }
        if (request.getAggregateProxy() != null) {
            AggregateProxy aggregateProxy = request.getAggregateProxy();
            kStream = getkStreamAggregate(aggregateProxy.getWindows(), aggregateProxy.getInitializer(), aggregateProxy.getAggregator(), aggregateProxy.getMaterialized(), kStream);
        }
        if (request.getAfterKeyValueMapper() != null) {
            kStream = kStream.map(request.getAfterKeyValueMapper());
        }
        if (request.getAfterFilter() != null) {
            kStream = kStream.filter(request.getAfterFilter());
        }
        if (request.getProcessor() != null) {
            kStream.process(() -> request.getProcessor());
        }
        if (request.getTopicOut() != null) {
            for (String topic : request.getTopicOut()) {
                kStream.to(topic, Produced.with(request.getKeyOutSerde(), request.getValueOutSerde()));
            }
        }
        KafkaStreams kafkaStreams = createKafkaStream(streamName, builder, request.getBrokerConfig());
        return new KafkaStreamContainer(streamName, kafkaStreams, kStream, request.getBrokerConfig(), null, builder.build());
    }

    private KStream getkStreamAggregate(Windows windows, InitializerProxy initializer, AggregatorProxy aggregator, Materialized materialized, KStream kStream) {
        KGroupedStream kGroupedStream = kStream.groupByKey();

        if (windows != null) {
            kStream = getKstreamWindowedAggregate(kGroupedStream, windows, initializer, aggregator, materialized);
        } else {
            kStream = getKstreamAggregate(kGroupedStream, initializer, aggregator, materialized);
        }
        return kStream;
    }

    private <K, V> KStream getKstreamWindowedAggregate(KGroupedStream kGroupedStream, Windows windows, InitializerProxy initializer, AggregatorProxy aggregator, Materialized materialized) {
        KTable<Windowed<K>, V> aggregate = kGroupedStream
                .windowedBy(windows)
                .aggregate(
                        () -> initializer.apply()
                        , (k, v, va) -> aggregator.apply(k, v, va)
                        , materialized);

        KStream KStreamOut = aggregate
                .toStream((k, v) -> k.key());

        return KStreamOut;
    }


    private KStream getKstreamAggregate(KGroupedStream kGroupedStream, InitializerProxy initializer, AggregatorProxy aggregator, Materialized materialized) {
        KStream KStreamOut = kGroupedStream
                .aggregate(() -> initializer.apply()
                        , (k, v, va) -> aggregator.apply(k, v, va)
                        , materialized)
                .toStream();

        return KStreamOut;
    }

    public KafkaStreams createKafkaStream(String streamName, StreamsBuilder streamsBuilder, KafkaBrokerConfig overrideKafkaBrokerConfig) {
        return createKafkaStream(streamName, streamsBuilder.build(), overrideKafkaBrokerConfig);
    }

    public KafkaStreamContainer createKafkaStreamContainer(String streamName, StreamsBuilder streamsBuilder, KafkaBrokerConfig kafkaBrokerConfig) {
        KafkaStreams kafkaStreams = createKafkaStream(streamName, streamsBuilder, kafkaBrokerConfig);
        return new KafkaStreamContainer(streamName, kafkaStreams, null, kafkaBrokerConfig, null, streamsBuilder.build());
    }

    public KafkaStreamContainer createKafkaStreamContainer(String streamName, Topology topology, KafkaBrokerConfig kafkaBrokerConfig) {
        KafkaStreams kafkaStreams = createKafkaStream(streamName, topology, kafkaBrokerConfig);
        return new KafkaStreamContainer(streamName, kafkaStreams, null, kafkaBrokerConfig, null, topology);
    }

    private KafkaStreams createKafkaStream(String streamName, Topology topology, KafkaBrokerConfig overrideKafkaBrokerConfig) {
        KafkaProxyClientSupplier kafkaProxyClientSupplier = new KafkaProxyClientSupplier(streamName, listenerConfig);
        KafkaStreams kafkaStreams = new KafkaStreams(topology, KafkaUtils.getKafkaStreamsConfigs(overrideKafkaBrokerConfig != null ? overrideKafkaBrokerConfig : kafkaBrokerConfig, streamName), kafkaProxyClientSupplier);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        return kafkaStreams;
    }
}
