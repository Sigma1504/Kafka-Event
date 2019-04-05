package com.accor.tech.config;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "streamconfig")
public class ListenerConfig {
    private String instanceName;
    @Builder.Default
    private Long punctuationTime = new Long(15);
    private HashMap<String, String> mapLabels;
    @Builder.Default
    private List<String> listGroupMetricsKafka = new ArrayList<>();
    @Builder.Default
    private Boolean metricStream = false;
    @Builder.Default
    private Boolean logAllProduce = false;
    @Builder.Default
    private HashMap<String, String> mapTopicProduceLog = new HashMap<>();
}
