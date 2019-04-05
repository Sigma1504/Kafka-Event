package com.accor.tech.admin.config;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "kafka.admin")
public class KafkaAdminConfig {
    @Builder.Default
    private Integer topicDefaultReplica = 1;
    @Builder.Default
    private Integer topicDefaultPartition = 2;
    @Builder.Default
    private boolean topicSecured = false;
    @Builder.Default
    private Integer topicDefaultRetentionHours = 96;
}
