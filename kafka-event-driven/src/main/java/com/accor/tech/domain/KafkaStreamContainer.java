package com.accor.tech.domain;


import com.accor.tech.admin.config.KafkaBrokerConfig;
import com.accor.tech.admin.domain.KafkaStreamInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Date;


@AllArgsConstructor
@Slf4j
public class KafkaStreamContainer implements Runnable{
    @Getter
    private final String streamName;
    private final KafkaStreams kafkaStreams;
    @Getter
    private final KStream kStream;
    @Getter
    private final KafkaBrokerConfig kafkaBrokerConfig;
    @Getter
    private Date startTime;
    @Getter
    private final Topology topology;

    @Override
    public void run() {
        startStream();
    }

    public void startStream(){
        try {
            this.kafkaStreams.start();
            this.startTime = new Date(System.currentTimeMillis());
        } catch (Exception e) {
            log.error("Error during start stream {}", e);
        }
    }
    public void stopStream(){
        try {
            this.kafkaStreams.close();
        } catch (Exception e) {
            log.error("Error during stop stream {}", e);
        }
    }

    public KafkaStreamInfo getStreamInfo(){
        return new KafkaStreamInfo(streamName,kafkaStreams.state(), kafkaStreams.metrics());
    }

}
