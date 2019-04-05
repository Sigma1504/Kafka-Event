package com.accor.tech.service;

import com.accor.tech.config.ListenerConfig;
import com.accor.tech.domain.KafkaStreamContainer;
import com.accor.tech.utils.MetricsUtils;
import com.google.common.collect.Streams;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusTimer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static com.accor.tech.utils.MetricsUtils.SOCLE_EVENT_DRIVE;

@Component
@Slf4j
public class MetricService {

    private final ListenerConfig listenerConfig;

    public MetricService(ListenerConfig listenerConfig) {
        this.listenerConfig = listenerConfig;
    }

    public Map<String, Double> generateMetrics(KafkaStreamContainer kafkaStreamContainer) {
        Map<String, Double> mapMetrics = new HashMap<>();
        if (!listenerConfig.getListGroupMetricsKafka().isEmpty() &&
                kafkaStreamContainer.getStreamInfo().getMetrics() != null &&
                !kafkaStreamContainer.getStreamInfo().getMetrics().isEmpty()) {
            addMetricFromKafka(kafkaStreamContainer.getStreamInfo().getMetrics(), mapMetrics);
        }
        if (listenerConfig.getMetricStream()) {
            addMetricsFromMicrometer(kafkaStreamContainer.getStreamName(), mapMetrics);
        }
        return mapMetrics;
    }

    private void addMetricFromKafka(Map<MetricName, ? extends Metric> mapMetricsFromKafka, Map<String, Double> mapMetrics) {
        Streams.stream(mapMetricsFromKafka.entrySet().iterator())
                .filter(entry -> {
                    if (listenerConfig.getListGroupMetricsKafka().contains("*")) {
                        return true;
                    } else {
                        return listenerConfig.getListGroupMetricsKafka().contains(entry.getKey().group());
                    }
                })
                .forEach(entry -> mapMetrics.put(entry.getKey().group() + "-" + entry.getKey().name(), Double.valueOf(entry.getValue().value())));
    }

    private void addMetricsFromMicrometer(String streamName, Map<String, Double> mapMetrics) {
        CompositeMeterRegistry compositeMeterRegistry = Metrics.globalRegistry;
        compositeMeterRegistry.getRegistries().forEach(registry ->
                registry.getMeters().stream()
                        .filter(meter -> meter.getId().getTag(SOCLE_EVENT_DRIVE) != null)
                        .filter(meter -> meter.getId().getTag(MetricsUtils.STREAM_NAME).equals(streamName))
                        .forEach(meter -> {
                            meter.measure().forEach(measurement -> {
                                String key = SOCLE_EVENT_DRIVE + "_" + meter.getId().getTag(MetricsUtils.FUCNTION) + "_" + meter.getId().getTag(MetricsUtils.STEP);
                                if (meter.getId().getType() == Meter.Type.TIMER && meter instanceof PrometheusTimer) {
                                    PrometheusTimer prometheusTimer = (PrometheusTimer) meter;
                                    mapMetrics.put(key + "_count", Double.valueOf(prometheusTimer.count()));
                                    mapMetrics.put(key + "_sum", prometheusTimer.totalTime(prometheusTimer.baseTimeUnit()));
                                    mapMetrics.put(key + "_max", prometheusTimer.max(prometheusTimer.baseTimeUnit()));
                                } else {
                                    mapMetrics.put(key, measurement.getValue());
                                }
                            });
                        })
        );

    }

}
