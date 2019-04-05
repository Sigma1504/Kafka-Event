package com.accor.tech.api.internal.function;

import com.accor.tech.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Objects;

import static com.accor.tech.utils.MetricsUtils.*;

@Slf4j
public class AggregatorProxy<K, V, VA> implements Aggregator<K, V, VA> {

    private Aggregator<K, V, VA> aggregator;
    private String streamName;
    private String functionName;

    public AggregatorProxy(String streamName, Aggregator aggregator) {
        this.aggregator = aggregator;
        this.streamName = streamName;
        Objects.requireNonNull(streamName, "streamName can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        functionName = streamName.concat("_Initializer");

    }

    @Override
    public VA apply(K key, V value, VA aggregate) {
        VA result = null;
        boolean exception = true;
        MetricsUtils.incrementsMetric(streamName, functionName, STEP_BEGIN);
        long begin = System.nanoTime();
        try {
            result = aggregator.apply(key, value, aggregate);
            MetricsUtils.timerMetric(streamName, functionName, APPLY, System.nanoTime() - begin);
            exception = false;
        } catch (RuntimeException e) {
            log.error("Error in Aggregate for Stream {0} : exception {1}", streamName, e);
            MetricsUtils.incrementsMetricError(streamName, functionName, e.getClass().getName());
        }
        if (!exception) {
            MetricsUtils.incrementsMetric(streamName, streamName.concat("Aggregate"), STEP_END);
        }
        return result;
    }
}
