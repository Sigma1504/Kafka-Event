package com.accor.tech.api;

import com.accor.tech.api.internal.KafkaStreamContainerBuilder;
import com.accor.tech.api.internal.StreamExecutor;
import com.accor.tech.api.internal.StreamingRequestProxy;
import com.accor.tech.domain.KafkaStreamContainer;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;


@Component
@AllArgsConstructor
public class StreamApi {

    private final StreamExecutor streamExecutor;
    private final KafkaStreamContainerBuilder kafkaStreamContainerBuilder;


    public KStream createStream(StreamingRequest streamingRequest) throws TopicExistsException {
        StreamingRequestProxy requestProxy = new StreamingRequestProxy(streamingRequest);
        KafkaStreamContainer kafkaStreamContainer = kafkaStreamContainerBuilder.createKafkaStreamContainer(requestProxy);
        streamExecutor.addStream(streamingRequest.getStreamName(), kafkaStreamContainer);
        return kafkaStreamContainer.getKStream();
    }


    public KStream createAndStartStream(StreamingRequest streamingRequest) throws TopicExistsException {
        StreamingRequestProxy requestProxy = new StreamingRequestProxy(streamingRequest);
        KafkaStreamContainer kafkaStreamContainer = kafkaStreamContainerBuilder.createKafkaStreamContainer(requestProxy);
        streamExecutor.addAndStartStream(streamingRequest.getStreamName(), kafkaStreamContainer);
        return kafkaStreamContainer.getKStream();
    }

    public void createAndStartStream(Topology topology, String streamName) {
        streamExecutor.addAndStartStream(streamName, kafkaStreamContainerBuilder.createKafkaStreamContainer(streamName, topology, null));
    }

    public void createStream(StreamsBuilder streamsBuilder, String streamName) {
        streamExecutor.addStream(streamName, kafkaStreamContainerBuilder.createKafkaStreamContainer(streamName, streamsBuilder, null));
    }


    public void startStream(String streamName){
        streamExecutor.startStream(streamName);
    }

    public void startAllStream(){
        streamExecutor.startAll();
    }


    public void stopStream (String streamName){
        streamExecutor.stopStream(streamName);
    }


    public void stopAllStream (){
        streamExecutor.stopAll();
    }


    public void deleteStream (String streamName){
        streamExecutor.removeAndStopStream(streamName);
    }


    public void deleteAllStream (){
        streamExecutor.removeAndStopAllStream();
    }

}
