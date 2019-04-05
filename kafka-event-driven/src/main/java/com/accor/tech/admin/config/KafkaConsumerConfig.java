package com.accor.tech.admin.config;


import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

@Getter
@Setter
public class KafkaConsumerConfig {

    @Builder.Default
    private String keySerdesClass = Serdes.String().getClass().getName();
    @Builder.Default
    private String valueSerdesClass = Serdes.String().getClass().getName();
    @Builder.Default
    private int commitIntervalMsConfig = 100;
    @Builder.Default
    private int cacheMaxBytesBufferingConfig = 0;
    @Builder.Default
    private boolean transaction = false;
    @Builder.Default
    private String autoOffsetResetPolicy = "earliest";
    @Builder.Default
    private int sessionTimeOut = 10000;
    @Builder.Default
    private int maxPollRecords = 500;
    @Builder.Default
    private int maxPollInterval = 300000;
    @Builder.Default
    private int pollInterval = 100;
    @Builder.Default
    private String transactionLevel = "read_committed";
    @Builder.Default
    private String topicCleanupPolicy = TopicConfig.CLEANUP_POLICY_COMPACT;
    @Builder.Default
    private String timeExtractor = WallclockTimestampExtractor.class.getName();
    @Builder.Default
    private String deserializationExceptionHandler = LogAndContinueExceptionHandler.class.getName();
    @Builder.Default
    private int threadNumber = 10;


}
