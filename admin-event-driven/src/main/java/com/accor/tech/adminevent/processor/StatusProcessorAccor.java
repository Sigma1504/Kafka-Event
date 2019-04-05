package com.accor.tech.adminevent.processor;

import com.accor.tech.api.internal.StreamExecutor;
import com.accor.tech.config.ListenerConfig;
import com.accor.tech.domain.EventActionStream;
import com.google.common.collect.Lists;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import static com.accor.tech.utils.MetricsUtils.INSTANCE_NAME;
import static com.accor.tech.utils.MetricsUtils.STREAM_NAME;

@Slf4j
public class StatusProcessorAccor implements Processor<String, EventActionStream> {

    private ProcessorContext context;

    public final String TYPE_STATUS = "typeStatus";
    private final StreamExecutor streamExecutor;
    private final ListenerConfig listenerConfig;

    public StatusProcessorAccor(StreamExecutor streamExecutor, ListenerConfig listenerConfig) {
        this.streamExecutor = streamExecutor;
        this.listenerConfig = listenerConfig;
    }

    public void init(ProcessorContext context) {
        this.context = context;
    }

    public void process(String key, EventActionStream eventActionStream) {
        log.debug("Status processor for {}", eventActionStream);
        if (eventActionStream != null) {
            Metrics.counter("listener-status",
                    Lists.newArrayList(
                            Tag.of(STREAM_NAME, eventActionStream.getStreamName()),
                            Tag.of(INSTANCE_NAME, eventActionStream.getInstanceName() != null ? eventActionStream.getInstanceName() : "unknown"),
                            Tag.of(TYPE_STATUS, eventActionStream.getEventStatus() != null ? eventActionStream.getEventStatus().name() : "unknown"))
            ).increment();
        }
    }

    public void close() {
        // nothing to do
    }
}
