package com.accor.tech.api.internal.function;

import com.accor.tech.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Objects;

import static com.accor.tech.utils.MetricsUtils.APPLY;
import static com.accor.tech.utils.MetricsUtils.ERROR;

@Slf4j
public class KeyValueMapperProxy<K, V, VR> implements KeyValueMapper<K, V, VR> {

    private KeyValueMapper<K, V, VR> keyValueMapperReference;
    private String keyValueMapperName;
    private String streamName;

    public KeyValueMapperProxy(String streamName, String keyValueMapperName, KeyValueMapper<K, V, VR> keyValueMapperReference) {
        this.streamName = streamName;
        this.keyValueMapperName = keyValueMapperName;
        this.keyValueMapperReference = keyValueMapperReference;
        Objects.requireNonNull(keyValueMapperReference, "keyValueMapperReference can't be null");
        Objects.requireNonNull(keyValueMapperName, "keyValueMapperName can't be null");
        Objects.requireNonNull(streamName, "streamName can't be null");
    }

    @Override
    public VR apply(K key, V value) {
        long start = System.nanoTime();
        try {
            VR result = keyValueMapperReference.apply(key, value);
            MetricsUtils.timerMetric(streamName, keyValueMapperName, APPLY, System.nanoTime() - start);
            return result;
        } catch (RuntimeException e) {
            log.error("Error in keyValueMapperName " + keyValueMapperName + " for streamName " + streamName + " exception " + e);
            MetricsUtils.incrementsMetric(streamName, keyValueMapperName, ERROR);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
