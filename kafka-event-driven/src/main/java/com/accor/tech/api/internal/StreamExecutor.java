package com.accor.tech.api.internal;

import com.accor.tech.config.ListenerConfig;
import com.accor.tech.domain.KafkaStreamContainer;
import com.accor.tech.domain.TypeStatus;
import com.accor.tech.listener.ProducerEventService;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Component
@Lazy(value = false)
@Slf4j
public class StreamExecutor {

    private static final int NUM_CONSUMERS = 10;
    private final ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS);
    @Getter
    @Builder.Default
    private final Map<String, KafkaStreamContainer> streamsContainerMap = new HashMap<>();
    private final ProducerEventService producerEventService;
    private final ListenerConfig listenerConfig;
    private final KafkaStreamContainerBuilder kafkaStreamContainerBuilder;

    public StreamExecutor(ProducerEventService producerEventService, ListenerConfig listenerConfig, KafkaStreamContainerBuilder kafkaStreamContainerBuilder) {
        this.producerEventService = producerEventService;
        this.listenerConfig = listenerConfig;
        this.kafkaStreamContainerBuilder = kafkaStreamContainerBuilder;
    }

    public void addStream (String streamName, KafkaStreamContainer container){
        streamsContainerMap.put(streamName, container);
    }

    public void addAndStartStream (String streamName, KafkaStreamContainer container){
        addStream(streamName,container);
        startStream(streamName);
    }

    public void removeAndStopStream(String streamName) {
        KafkaStreamContainer streamContainer =  streamsContainerMap.get(streamName);
        if(streamContainer!=null){
            stopStream(streamContainer);
            streamsContainerMap.remove(streamName);
        }else{
            log.error("Stream Container does'nt exist {}",streamName);
        }
    }

    public void startAll (){
        streamsContainerMap.forEach((k,v) -> {
            startStream(v.getStreamName());
        });
    }

    public void startStream(String streamName) {
        KafkaStreamContainer streamContainer = streamsContainerMap.get(streamName);
        if (streamContainer != null) {
            startStreamFromStatus(streamContainer);
        } else {
            log.error("Stream Container {} doesn't exist ", streamName);
        }
    }

    public void stopStream(String streamName){
        KafkaStreamContainer streamContainer =  streamsContainerMap.get(streamName);
        if (streamContainer !=null){
            log.info("stop Stream {} ", streamName);
            stopStream(streamContainer);
        }
    }

    public void removeAndStopAllStream() {
        streamsContainerMap.forEach((k, kafkaStreamContainer) -> {
            removeAndStopStream(k);
        });
    }

    public void stopAll() {
        streamsContainerMap.forEach((k, kafkaStreamContainer) -> {
            log.info("Stop Stream {}", k);
            stopStream(kafkaStreamContainer);
        });
    }

    private void stopStream(KafkaStreamContainer kafkaStreamContainer) {
        KafkaStreamContainer streamContainer = streamsContainerMap.get(kafkaStreamContainer.getStreamName());
        if (streamContainer != null) {
            kafkaStreamContainer.stopStream();
            //producer stopped
            producerEventService.produceStatus(TypeStatus.STOPPED, kafkaStreamContainer.getStreamName(), listenerConfig.getInstanceName(), listenerConfig.getMapLabels());
        } else {
            log.error("Stream Container {} doesn't exist ", kafkaStreamContainer.getStreamName());
        }
    }

    private void startStreamFromStatus(KafkaStreamContainer kafkaStreamContainer) {
        if (isReadyForRunning(kafkaStreamContainer)) {
            log.info("start Stream {} ", kafkaStreamContainer.getStreamName());
            executor.submit(kafkaStreamContainer);
            //producer started
            producerEventService.produceStatus(TypeStatus.STARTED, kafkaStreamContainer.getStreamName(), listenerConfig.getInstanceName(), listenerConfig.getMapLabels());
        } else if (isNotRunning(kafkaStreamContainer)) {
            //recreate from topology
            recreateFromNotRunning(kafkaStreamContainer);
        } else {
            log.info("Stream {} doesn't start because status Stream is not correct {}", kafkaStreamContainer.getStreamName(), kafkaStreamContainer.getStreamInfo().getStreamStatus().name());
        }
    }

    private void recreateFromNotRunning(KafkaStreamContainer kafkaStreamContainer) {
        KafkaStreamContainer kafkaStreamContainerRecreated = kafkaStreamContainerBuilder.createKafkaStreamContainer(kafkaStreamContainer.getStreamName(), kafkaStreamContainer.getTopology(), kafkaStreamContainer.getKafkaBrokerConfig());
        streamsContainerMap.put(kafkaStreamContainer.getStreamName(), kafkaStreamContainerRecreated);
        startStream(kafkaStreamContainer.getStreamName());
    }

    private boolean isReadyForRunning(KafkaStreamContainer kafkaStreamContainer) {
        return kafkaStreamContainer.getStreamInfo().getStreamStatus() == KafkaStreams.State.CREATED ||
                kafkaStreamContainer.getStreamInfo().getStreamStatus() == KafkaStreams.State.ERROR;
    }

    private boolean isNotRunning(KafkaStreamContainer kafkaStreamContainer) {
        return kafkaStreamContainer.getStreamInfo().getStreamStatus() == KafkaStreams.State.NOT_RUNNING;
    }

    public Map<String, String> getStreamsStatus (){
        return streamsContainerMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getStreamInfo().getStreamStatus().name()));
    }

    public String getStreamStatus(String streamName){
        KafkaStreamContainer streamContainer =  streamsContainerMap.get(streamName);
        if (streamContainer !=null){
            return streamContainer.getStreamInfo().getStreamStatus().name();
        }
        return null;
    }

}
