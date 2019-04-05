package com.accor.tech.api.internal;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class HeaderUtils {

    public final static String HEADER_CORRELATION_ID = "correlationId";
    public final static String HEADER_TIMESTAMP_CONSUMER_CORRELATION_ID = "timestampConsumerCorrelationId";
    public final static String HEADER_STREAM_NAME = "streamName";
    public final static String HEADER_TOPIC_IN = "topic-in";
    public final static String HEADER_TOPIC_ORIGINAL = "topic-original";
    public final static String NO_VALUE = "noValue";

    public static String extractHeader(Headers headers, String key) {
        if (key != null) {
            String result = Arrays.asList(headers.toArray()).stream()
                    .filter(itemHeader -> itemHeader.key().equals(key))
                    .map(itemHeader -> new String(itemHeader.value()))
                    .collect(Collectors.joining(","));
            return result != null && !result.equals("") ? result : NO_VALUE;
        }
        return NO_VALUE;
    }


    public static void addHeaderIfNotExist(Headers headers, String streamName, String topic) {
        List<Header> headerList = Arrays.asList(headers.toArray());
        if (!headerList.stream().anyMatch(header -> header.key().equals(HEADER_CORRELATION_ID))) {
            headers.add(new RecordHeader(HEADER_CORRELATION_ID, UUID.randomUUID().toString().getBytes()));
        }
        if (!headerList.stream().anyMatch(header -> header.key().equals(HEADER_STREAM_NAME))) {
            headers.add(new RecordHeader(HEADER_STREAM_NAME, streamName.getBytes()));
        }
        if (!headerList.stream().anyMatch(header -> header.key().equals(HEADER_TOPIC_ORIGINAL))) {
            headers.add(new RecordHeader(HEADER_TOPIC_ORIGINAL, topic.getBytes()));
        }
    }


}
