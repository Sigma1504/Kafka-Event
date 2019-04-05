package com.accor.tech.api.internal.function;

import com.accor.tech.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Objects;

import static com.accor.tech.utils.MetricsUtils.*;

@Slf4j
public class PredicateProxy<K, V> implements Predicate<K, V> {

    private Predicate<K, V> predicateReference;
    private String predicateName;
    private String streamName;

    public PredicateProxy(String streamName, String predicateName, Predicate<K, V> predicateReference) {
        this.streamName = streamName;
        this.predicateName = predicateName;
        this.predicateReference = predicateReference;
        Objects.requireNonNull(predicateReference, "predicateReference can't be null");
        Objects.requireNonNull(predicateName, "predicateName can't be null");
        Objects.requireNonNull(streamName, "streamName can't be null");

    }

    @Override
    public boolean test(K key, V value) {
        MetricsUtils.incrementsMetric(streamName, predicateName, STEP_BEGIN);
        Boolean resultFilter = false;
        long start = System.nanoTime();
        try {
            resultFilter = predicateReference.test(key, value);
            MetricsUtils.timerMetric(streamName, predicateName, APPLY, System.nanoTime() - start);
        } catch (RuntimeException e) {
            log.error("Error in predicateName " + predicateName + " for streamName " + streamName + " exception " + e);
            MetricsUtils.incrementsMetric(streamName, predicateName, ERROR);
            throw new RuntimeException(e.getMessage(), e);
        }
        if (resultFilter) {
            MetricsUtils.incrementsMetric(streamName, predicateName, STEP_END);
        }
        return resultFilter;
    }

}
