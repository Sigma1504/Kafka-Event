package com.accor.tech.api.internal.function;
/**
 * Useful??
 */

import com.accor.tech.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Initializer;

import java.util.Objects;

import static com.accor.tech.utils.MetricsUtils.*;

@Slf4j
public class InitializerProxy<VA> implements Initializer<VA> {

    private Initializer<VA> initializer;
    private String streamName;
    private String functionName;

    public InitializerProxy(String streamName, Initializer initializer) {
        this.initializer = initializer;
        this.streamName = streamName;
        Objects.requireNonNull(streamName, "streamName can't be null");
        Objects.requireNonNull(initializer, "initializer can't be null");
        functionName = streamName.concat("_Initializer");

    }

    @Override
    public VA apply() {
        VA result = null;
        boolean exception = true;
        MetricsUtils.incrementsMetric(streamName, functionName, STEP_BEGIN);
        long begin = System.nanoTime();
        try {
            result = initializer.apply();
            MetricsUtils.timerMetric(streamName, functionName, APPLY, System.nanoTime() - begin);
            exception = false;
        } catch (RuntimeException e) {
            log.error("Error in Initializer for Stream {0} : exception {1}", streamName, e);
            MetricsUtils.incrementsMetricError(streamName, functionName, e.getCause().getMessage());
        }
        if (!exception) {
            MetricsUtils.incrementsMetric(streamName, streamName.concat("Initializer"), STEP_END);
        }
        return result;
    }
}
