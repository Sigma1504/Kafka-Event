package com.accor.tech.api.internal.function;

import com.accor.tech.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Objects;

import static com.accor.tech.utils.MetricsUtils.APPLY;
import static com.accor.tech.utils.MetricsUtils.ERROR;

@Slf4j
public class ValueMapperProxy<V, VR> implements ValueMapper<V, VR> {

    private ValueMapper<V, VR> valueMapperReference;
    private String valueMapperName;
    private String streamName;

    public ValueMapperProxy(String streamName, String valueMapperName, ValueMapper<V, VR> valueMapperReference) {
        this.streamName = streamName;
        this.valueMapperName = valueMapperName;
        this.valueMapperReference = valueMapperReference;
        Objects.requireNonNull(valueMapperName, "valueMapperName can't be null");
        Objects.requireNonNull(valueMapperReference, "valueMapperReference can't be null");
        Objects.requireNonNull(streamName, "streamName can't be null");
    }

    @Override
    public VR apply(V value) {
        long start = System.nanoTime();
        try {
            VR result = valueMapperReference.apply(value);
            MetricsUtils.timerMetric(streamName, valueMapperName, APPLY, System.nanoTime() - start);
            return result;
        } catch (RuntimeException e) {
            log.error("Error in ValueMapper " + valueMapperReference + " for streamName " + streamName + " exception " + e);
            MetricsUtils.incrementsMetric(streamName, valueMapperName, ERROR);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
