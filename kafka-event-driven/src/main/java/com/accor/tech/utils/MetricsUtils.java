package com.accor.tech.utils;

import com.google.common.collect.Lists;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.util.concurrent.TimeUnit;

public class MetricsUtils {

    public final static String STEP = "step";
    public final static String FUCNTION = "function";
    public final static String STREAM_NAME = "streamName";
    public final static String SOCLE_EVENT_DRIVE = "socle-event-driven";
    public final static String INSTANCE_NAME = "instanceName";
    public final static String TYPE_ACTION = "typeAction";
    public final static String APPLY = "apply";
    public final static String ERROR = "error";
    public final static String TOPIC_ORIGINAL = "topicOriginal";
    public final static String TOPIC_FINAL = "topicFinal";
    public final static String STEP_BEGIN = "pre";
    public final static String STEP_END = "post";
    public final static String ERROR_CAUSE = "errorCause";


    public static void incrementsMetric(String streamName, String name, String step) {
        Metrics.counter(streamName + "_" + name,
                Lists.newArrayList(
                        Tag.of(STEP, step),
                        Tag.of(FUCNTION, name),
                        Tag.of(STREAM_NAME, streamName),
                        Tag.of(SOCLE_EVENT_DRIVE, "true")
                )
        ).increment();
    }

    public static void timerMetricTopicToTopic(String streamName, String topicOriginal, String topicFinal, long duration) {
        Metrics.timer(topicOriginal + "-to-" + topicFinal,
                Lists.newArrayList(
                        Tag.of(TOPIC_ORIGINAL, topicOriginal),
                        Tag.of(TOPIC_FINAL, topicFinal),
                        Tag.of(FUCNTION, "topic-to-topic"),
                        Tag.of(STREAM_NAME, streamName),
                        Tag.of(SOCLE_EVENT_DRIVE, "true")
                )
        ).record(duration, TimeUnit.MILLISECONDS);
    }

    public static void timerMetric(String streamName, String name, String step, long duration) {
        Metrics.timer(streamName + "_" + name,
                Lists.newArrayList(
                        Tag.of(STEP, step),
                        Tag.of(FUCNTION, name),
                        Tag.of(STREAM_NAME, streamName),
                        Tag.of(SOCLE_EVENT_DRIVE, "true")
                )
        ).record(duration, TimeUnit.NANOSECONDS);
    }

    public static void incrementsMetricError(String streamName, String name, String cause) {
        Metrics.counter(streamName + "_" + name,
                Lists.newArrayList(
                        Tag.of(STEP, ERROR),
                        Tag.of(ERROR_CAUSE, cause),
                        Tag.of(FUCNTION, name),
                        Tag.of(STREAM_NAME, streamName),
                        Tag.of(SOCLE_EVENT_DRIVE, "true")
                )
        ).increment();
    }
}
