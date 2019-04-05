package com.accor.tech.processor;

import com.accor.tech.api.internal.StreamExecutor;
import com.accor.tech.config.ListenerConfig;
import com.accor.tech.domain.EventActionStream;
import com.google.common.collect.Lists;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.HashMap;

import static com.accor.tech.listener.ListenerActionService.STORE_ACTION_ACCOR;
import static com.accor.tech.listener.ListenerActionService.TECHNICAL_STREAM_NAME_ACCOR;
import static com.accor.tech.utils.MetricsUtils.*;

@Slf4j
public class ActionProcessorAccor implements Processor<String, EventActionStream> {

    public final static String STATUS_ACCOR_STREAM = "status-accor-stream";
    private ProcessorContext context;
    private KeyValueStore<String, EventActionStream> kvStore;

    private final StreamExecutor streamExecutor;
    private final ListenerConfig listenerConfig;

    public ActionProcessorAccor(StreamExecutor streamExecutor, ListenerConfig listenerConfig) {
        this.streamExecutor = streamExecutor;
        this.listenerConfig = listenerConfig;
    }

    public void init(ProcessorContext context) {
        this.context = context;
        this.kvStore = (KeyValueStore) context.getStateStore(STORE_ACTION_ACCOR);

        this.context.schedule(Duration.ofSeconds(listenerConfig.getPunctuationTime()), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            KeyValueIterator<String, EventActionStream> iter = this.kvStore.all();
            while (iter.hasNext()) {
                KeyValue<String, EventActionStream> entry = iter.next();
                //action
                actionStream(entry);
            }
            iter.close();
            // commit the current processing progress
            context.commit();
        });
    }

    public void process(String key, EventActionStream eventActionStream) {
        log.debug("process for {}", eventActionStream);
        if (eventActionStream != null) {
            Metrics.counter("listener-action",
                    Lists.newArrayList(
                            Tag.of(STREAM_NAME, eventActionStream.getStreamName()),
                            Tag.of(INSTANCE_NAME, listenerConfig.getInstanceName() != null ? listenerConfig.getInstanceName() : "unknown"),
                            Tag.of(TYPE_ACTION, eventActionStream.getTypeAction() != null ? eventActionStream.getTypeAction().name() : "unknown")
                    )
            ).increment();
            switch (eventActionStream.getTypeAction()) {
                case START:
                    checkMessageReceive(eventActionStream);
                    break;
                case STOP:
                    checkMessageReceive(eventActionStream);
                    break;
                case START_ALL:
                    checkMessageReceive(eventActionStream);
                    break;
                case FORCE_STOP:
                    log.info("Listener force stop {} dateGeneration {}", eventActionStream.getStreamName(), eventActionStream.getDateGeneration());
                    streamExecutor.removeAndStopStream(eventActionStream.getStreamName());
                    break;
                case DELETE:
                    checkMessageReceive(eventActionStream);
                    break;
                case DELETE_ALL:
                    checkMessageReceive(eventActionStream);
                    break;
                case STOP_ALL:
                    checkMessageReceive(eventActionStream);
                    break;
                default:
                    log.error("Type Action not recognized {}", eventActionStream);
            }
        }
    }

    public void close() {
        // nothing to do
    }

    private void checkMessageReceive(EventActionStream eventActionStream) {
        if (computeStreamName(eventActionStream.getStreamName()) && computeMapLabels(eventActionStream.getMapLabels()) && computeInstanceName(eventActionStream.getInstanceName())) {
            log.debug("checkMessageReceive stream {} ", eventActionStream);
            kvStore.put(eventActionStream.getStreamName(), eventActionStream);
        }
    }

    private void actionStream(KeyValue<String, EventActionStream> entry) {
        switch (entry.value.getTypeAction()) {
            case DELETE:
                counterMetrics(entry.value);
                streamExecutor.removeAndStopStream(entry.value.getStreamName());
            case DELETE_ALL:
                counterMetrics(entry.value);
                streamExecutor.removeAndStopAllStream();
            case STOP_ALL:
                counterMetrics(entry.value);
                streamExecutor.stopAll();
            case START:
                counterMetrics(entry.value);
                streamExecutor.startStream(entry.value.getStreamName());
                break;
            case START_ALL:
                counterMetrics(entry.value);
                streamExecutor.startAll();
                break;
            case STOP:
                counterMetrics(entry.value);
                streamExecutor.stopStream(entry.value.getStreamName());
                break;
            default:
                log.error("Type Action not recognized {}", entry.value);
        }
        //clean
        kvStore.delete(entry.key);
    }

    private void counterMetrics(EventActionStream eventActionStream) {
        Metrics.counter("process-action",
                Lists.newArrayList(
                        Tag.of(STREAM_NAME, eventActionStream.getStreamName()),
                        Tag.of(INSTANCE_NAME, listenerConfig.getInstanceName() != null ? listenerConfig.getInstanceName() : "unknown"),
                        Tag.of(TYPE_ACTION, eventActionStream.getTypeAction().name())
                )
        ).increment();
    }

    private boolean computeStreamName(String streamName) {
        if (streamName != null &&
                streamExecutor.getStreamsContainerMap().containsKey(streamName) &&
                !streamName.equals(TECHNICAL_STREAM_NAME_ACCOR) && !streamName.equals(STATUS_ACCOR_STREAM)) {
            return true;
        }
        return false;
    }

    private boolean computeInstanceName(String instanceName) {
        if (instanceName != null && !instanceName.equals("")) {
            if (listenerConfig.getInstanceName() != null && !listenerConfig.getInstanceName().equals(instanceName)) {
                return false;
            }
        }
        return true;
    }

    private boolean computeMapLabels(HashMap<String, String> mapLabels) {
        if (mapLabels != null && !mapLabels.isEmpty()) {
            for (String key : mapLabels.keySet()) {
                if (listenerConfig.getMapLabels().get(key) != null && !listenerConfig.getMapLabels().get(key).equals(mapLabels.get(key))) {
                    return false;
                }
            }
        }
        return true;
    }

}
