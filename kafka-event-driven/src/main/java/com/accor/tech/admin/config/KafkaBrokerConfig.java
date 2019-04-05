package com.accor.tech.admin.config;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaBrokerConfig {

    private String bootStrapServers;
    private String applicationName;
    // security compliance
    @Builder.Default
    private KafkaSecurityConfig securityConfig = new KafkaSecurityConfig();
    @Builder.Default
    private KafkaProducerConfig producerConfig = new KafkaProducerConfig();
    @Builder.Default
    private KafkaConsumerConfig consumerConfig = new KafkaConsumerConfig();


}
