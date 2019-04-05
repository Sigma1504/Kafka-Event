package com.accor.tech.api.internal.function;

import com.accor.tech.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ForeachAction;

import java.util.Objects;

import static com.accor.tech.utils.MetricsUtils.APPLY;
import static com.accor.tech.utils.MetricsUtils.ERROR;

@Slf4j
public class ForeachActionProxy<K, V> implements ForeachAction<K, V> {

    private ForeachAction<K, V> foreachActionReference;
    private String foreachActionReferenceName;
    private String streamName;

    public ForeachActionProxy(String streamName, String foreachActionReferenceName, ForeachAction<K, V> foreachActionReference) {
        this.streamName = streamName;
        this.foreachActionReferenceName = foreachActionReferenceName;
        this.foreachActionReference = foreachActionReference;
        Objects.requireNonNull(foreachActionReference, "foreachActionReference can't be null");
        Objects.requireNonNull(foreachActionReferenceName, "foreachActionReferenceName can't be null");
        Objects.requireNonNull(streamName, "streamName can't be null");
    }

    @Override
    public void apply(K key, V value) {
        long start = System.nanoTime();
        try {
            foreachActionReference.apply(key, value);
            MetricsUtils.timerMetric(streamName, foreachActionReferenceName, APPLY, System.nanoTime() - start);
        } catch (RuntimeException e) {
            log.error("Error in foreachActionReferenceName " + foreachActionReferenceName + " for streamName " + streamName + " exception " + e);
            MetricsUtils.incrementsMetric(streamName, foreachActionReferenceName, ERROR);
            throw new RuntimeException(e.getMessage(), e);
        }
    }


}
