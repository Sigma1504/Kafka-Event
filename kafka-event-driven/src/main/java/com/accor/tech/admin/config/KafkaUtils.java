package com.accor.tech.admin.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaUtils {

    private final static String STREAMING_APPLCATION_PREFIX = "STREAM-PROCESS-";

    @Deprecated
    public static <K, V> Producer<K, V> kafkaProducer(String bootstrapServer, Class<? extends Serializer<K>> keySerializer, Class<? extends Serializer<V>> valueSerializer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
        return new KafkaProducer<>(props);
    }

    public static <K, V> Producer<K, V> kafkaProducer(KafkaBrokerConfig brokerConfig, Class<? extends Serializer<K>> keySerializer, Class<? extends Serializer<V>> valueSerializer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConfig.getBootStrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, brokerConfig.getProducerConfig().getAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, brokerConfig.getProducerConfig().getRetries());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, brokerConfig.getProducerConfig().getBatchSize());
        props.put(ProducerConfig.LINGER_MS_CONFIG, brokerConfig.getProducerConfig().getLinger());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, brokerConfig.getProducerConfig().getBufferMemory());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, brokerConfig.getProducerConfig().getRetryWaitTime());
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, brokerConfig.getProducerConfig().getMaxRequestByConnection());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, brokerConfig.getProducerConfig().getCompressionType());

        props.putAll(getKafkaSecurisation(brokerConfig));

        return new KafkaProducer<>(props);
    }

    public static <K, V> Producer<K, V> kafkaProducer(KafkaBrokerConfig brokerConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConfig.getBootStrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, brokerConfig.getProducerConfig().getAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, brokerConfig.getProducerConfig().getRetries());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, brokerConfig.getProducerConfig().getBatchSize());
        props.put(ProducerConfig.LINGER_MS_CONFIG, brokerConfig.getProducerConfig().getLinger());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, brokerConfig.getProducerConfig().getBufferMemory());
        Class keySerializer = getaClass(brokerConfig.getProducerConfig().getKeySerializerClass()) == null ? StringSerializer.class : getaClass(brokerConfig.getProducerConfig().getKeySerializerClass());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
        Class valueSerializer = getaClass(brokerConfig.getProducerConfig().getValueSerializerClass()) == null ? StringSerializer.class : getaClass(brokerConfig.getProducerConfig().getValueSerializerClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, brokerConfig.getProducerConfig().getRetryWaitTime());
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, brokerConfig.getProducerConfig().getMaxRequestByConnection());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, brokerConfig.getProducerConfig().getCompressionType());

        props.putAll(getKafkaSecurisation(brokerConfig));

        return new KafkaProducer<>(props);
    }

    public static Map<String, Object> buildDefaultConsumerConfig(KafkaBrokerConfig brokerConfig) {
        return getConsumerConfigs(brokerConfig, "default-construct-topic");
    }

    public static Map<String, Object> getConsumerConfigs(KafkaBrokerConfig brokerConfig, String streamName) {
        Map<String, Object> props = new HashMap<>();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConfig.getBootStrapServers());
        props.put(StreamsConfig.CLIENT_ID_CONFIG, brokerConfig.getApplicationName());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, STREAMING_APPLCATION_PREFIX.concat(streamName));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, brokerConfig.getConsumerConfig().getKeySerdesClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, brokerConfig.getConsumerConfig().getValueSerdesClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, brokerConfig.getConsumerConfig().getCommitIntervalMsConfig());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, brokerConfig.getConsumerConfig().getCacheMaxBytesBufferingConfig());
        props.put(StreamsConfig.TOPIC_PREFIX + TopicConfig.CLEANUP_POLICY_CONFIG, brokerConfig.getConsumerConfig().getTopicCleanupPolicy());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, brokerConfig.getConsumerConfig().getTimeExtractor());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, brokerConfig.getConsumerConfig().getDeserializationExceptionHandler());

        if (brokerConfig.getConsumerConfig().isTransaction()) {
            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        }
        props.put(StreamsConfig.POLL_MS_CONFIG, brokerConfig.getConsumerConfig().getPollInterval());
        props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.ISOLATION_LEVEL_CONFIG, brokerConfig.getConsumerConfig().getTransactionLevel());
        props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, brokerConfig.getConsumerConfig().getMaxPollInterval());
        props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, brokerConfig.getConsumerConfig().getSessionTimeOut());
        props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, brokerConfig.getConsumerConfig().getAutoOffsetResetPolicy());
        props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, brokerConfig.getConsumerConfig().getMaxPollRecords());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, brokerConfig.getConsumerConfig().getThreadNumber());
        props.putAll(getKafkaSecurisation(brokerConfig));

        return props;
    }

    private static Map<String, Object> getKafkaSecurisation(KafkaBrokerConfig brokerConfig) {
        Map<String, Object> securityProps = new HashMap<>();
        //securization
        if (brokerConfig.getSecurityConfig().getJaasConfig() != null)
            securityProps.put(SaslConfigs.SASL_JAAS_CONFIG, brokerConfig.getSecurityConfig().getJaasConfig());
        if (brokerConfig.getSecurityConfig().getSaslMechanism() != null)
            securityProps.put(SaslConfigs.SASL_MECHANISM, brokerConfig.getSecurityConfig().getSaslMechanism());
        if (brokerConfig.getSecurityConfig().getSecurityProtocol() != null)
            securityProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, brokerConfig.getSecurityConfig().getSecurityProtocol());
        if (brokerConfig.getSecurityConfig().getSslTrustoreLocation() != null)
            securityProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, brokerConfig.getSecurityConfig().getSslTrustoreLocation());
        if (brokerConfig.getSecurityConfig().getSslTrustorePwd() != null)
            securityProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, brokerConfig.getSecurityConfig().getSslTrustorePwd());
        if (brokerConfig.getSecurityConfig().getSslKeyStoreLocation() != null)
            securityProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, brokerConfig.getSecurityConfig().getSslKeyStoreLocation());
        if (brokerConfig.getSecurityConfig().getSslKeyStorePwd() != null)
            securityProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, brokerConfig.getSecurityConfig().getSslKeyStorePwd());
        if (brokerConfig.getSecurityConfig().getSslPrivateKeyPwd() != null)
            securityProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, brokerConfig.getSecurityConfig().getSslPrivateKeyPwd());

        return securityProps;
    }

    public static StreamsConfig getKafkaStreamsConfigs(KafkaBrokerConfig brokerConfig, String streamName) {
        return new StreamsConfig(getConsumerConfigs(brokerConfig, streamName));
    }

    private static Class<?> getaClass(String clazzPath) {
        try {
            return Class.forName(clazzPath);
        } catch (ClassNotFoundException e) {
            return null;
        }

    }

}
