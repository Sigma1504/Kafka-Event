package com.accor.tech.api.internal.function;

import lombok.Getter;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windows;

import java.util.Objects;

@Getter
public class AggregateProxy {

    private Windows windows;
    private InitializerProxy initializer;
    private AggregatorProxy aggregator;
    private Materialized materialized;

    public AggregateProxy(Windows windows, InitializerProxy initializer, AggregatorProxy aggregator, Materialized materialized) {
        this.windows = windows;
        this.initializer = initializer;
        this.aggregator = aggregator;
        this.materialized = materialized;
        Objects.requireNonNull(initializer, "Initializer can't be null");
        Objects.requireNonNull(aggregator, "Initializer can't be null");
        Objects.requireNonNull(materialized, "Initializer can't be null");
    }
}
