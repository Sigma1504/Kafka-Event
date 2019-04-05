package com.accor.tech.api.internal.function;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class ProcessorSupplierProxy<K, V> implements ProcessorSupplier<K, V> {

    private ProcessorProxy<K, V> processorReference;

    public ProcessorSupplierProxy(ProcessorProxy<K, V> processorReference) {
        this.processorReference = processorReference;
    }

    @Override
    public Processor<K, V> get() {
        return processorReference;
    }
}
