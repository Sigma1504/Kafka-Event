package com.accor.tech.admin.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams.State;

import java.util.Map;

@Getter
@AllArgsConstructor
@ToString
public class KafkaStreamInfo {

    private String streamName;
    private State streamStatus;
    private Map<MetricName, ? extends Metric> metrics;
}
