package com.accor.tech.admin.config;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.serialization.StringSerializer;

@Getter
@Setter
public class KafkaProducerConfig {

    @Builder.Default
    private String acks = "all";
    @Builder.Default
    private int retries = 1;
    @Builder.Default
    private long retryWaitTime = 10;
    @Builder.Default
    private int batchSize = 1;
    @Builder.Default
    private int maxRequestByConnection = 1;
    @Builder.Default
    private String keySerializerClass = StringSerializer.class.getName();
    @Builder.Default
    private String valueSerializerClass = StringSerializer.class.getName();
    @Builder.Default
    private int linger = 10;
    @Builder.Default
    private long bufferMemory = 33554432;
    @Builder.Default
    private String compressionType = "gzip";
    @Builder.Default
    private boolean transaction = false;





}
