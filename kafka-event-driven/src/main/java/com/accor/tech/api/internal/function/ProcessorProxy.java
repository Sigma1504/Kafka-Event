package com.accor.tech.api.internal.function;

import com.accor.tech.utils.MetricsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Objects;

import static com.accor.tech.utils.MetricsUtils.APPLY;
import static com.accor.tech.utils.MetricsUtils.ERROR;

@Slf4j
public class ProcessorProxy<K, V> implements Processor<K, V> {

    private Processor<K, V> processorReference;
    private String processorName;
    private String streamName;
    private ProcessorContext context;

    public ProcessorProxy(String streamName, String processorName, Processor<K, V> processorReference) {
        this.streamName = streamName;
        this.processorName = processorName;
        this.processorReference = processorReference;
        Objects.requireNonNull(processorReference, "processorReference can't be null");
        Objects.requireNonNull(processorName, "processorName can't be null");
        Objects.requireNonNull(streamName, "streamName can't be null");
    }


    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        processorReference.init(processorContext);
    }

    @Override
    public void process(K key, V value) {
        long start = System.nanoTime();
        try {
            processorReference.process(key, value);
            MetricsUtils.timerMetric(streamName, processorName, APPLY, System.nanoTime() - start);
        } catch (Exception e) {
            log.error("Error in processor " + processorName + " for streamName " + streamName + " exception " + e);
            MetricsUtils.incrementsMetric(streamName, processorName, ERROR);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        processorReference.close();
    }

}
