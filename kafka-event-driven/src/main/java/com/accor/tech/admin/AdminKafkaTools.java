package com.accor.tech.admin;

import com.accor.tech.admin.config.KafkaAdminConfig;
import com.accor.tech.admin.config.KafkaBrokerConfig;
import com.accor.tech.admin.config.KafkaUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

@Slf4j
@Component
public class AdminKafkaTools {

    private final KafkaBrokerConfig kafkaBrokerConfig;
    private final KafkaAdminConfig kafkaAdminConfig;

    public AdminKafkaTools(KafkaBrokerConfig kafkaBrokerConfig, KafkaAdminConfig kafkaAdminConfig) {
        this.kafkaBrokerConfig = kafkaBrokerConfig;
        this.kafkaAdminConfig = kafkaAdminConfig;
    }

    public void createDefaultTopic(String topic) {
        createTopic(topic, kafkaAdminConfig.getTopicDefaultPartition().intValue(), kafkaAdminConfig.getTopicDefaultReplica().shortValue());
    }

    public void createTopics(String... listTopic) {
        Stream.of(listTopic).filter(topic -> topic != null && !topic.equals(""))
                .forEach(topic -> createTopic(topic, kafkaAdminConfig.getTopicDefaultPartition().intValue(), kafkaAdminConfig.getTopicDefaultReplica().shortValue()));
    }

    public void createTopics(int partition, short replica, String... listTopic) {
        Stream.of(listTopic)
                .filter(topic -> topic != null && !topic.equals(""))
                .filter(topic -> partition > 0 && replica > 0)
                .forEach(topic -> createTopic(topic, partition, replica));
    }

    public void createTopic(String topic, int partition, short replica) {
        createTopic(topic, partition, replica, TopicConfig.CLEANUP_POLICY_COMPACT);
    }

    public void createTopic(String topic, String cleanUpPolicy) {
        createTopic(topic, kafkaAdminConfig.getTopicDefaultPartition().intValue(), kafkaAdminConfig.getTopicDefaultReplica().shortValue(), cleanUpPolicy);
    }

    public void deleteTopic(String... listTopic) {
        Stream.of(listTopic).filter(topic -> topic != null && !topic.equals(""))
                .forEach(topic -> deleteTopic(topic));
    }

    public void deleteTopic(String topic) {
        AdminClient adminClient = null;
        try {
            adminClient = KafkaAdminClient.create(KafkaUtils.buildDefaultConsumerConfig(kafkaBrokerConfig));
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topic));
            //sync
            deleteTopicsResult.values().get(topic).get();
        } catch (Exception e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                log.error("Topic {} not exists ", topic);
            } else if (e.getCause() instanceof TimeoutException) {
                log.error("Timeout for delete topic {}", topic);
            } else {
                log.error("Error during deleteTopic {} {}", topic, e);
            }
        } finally {
            adminClient.close();
        }
    }

    public void createTopic(String topic, int partition, short replica, String cleanUpPolicy) {
        AdminClient adminClient = null;
        try {
            adminClient = KafkaAdminClient.create(KafkaUtils.buildDefaultConsumerConfig(kafkaBrokerConfig));
            NewTopic newTopic = new NewTopic(topic, partition, replica);
            Map<String, String> configMap = new HashMap<>();
            configMap.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanUpPolicy);
            newTopic.configs(configMap);
            List<NewTopic> topics = Arrays.asList(newTopic);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(topics);
            //sync
            createTopicsResult.values().get(topic).get();
        } catch (TopicExistsException e) {
            log.error("Topic {} already exist !", topic);
        } catch (Exception e) {
            log.error("Error Creation Topic {} !", topic);
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } finally {
            adminClient.close();
        }
    }

    public boolean existTopics(Collection<String> topics) {
        Objects.requireNonNull(topics);
        AdminClient adminClient = null;
        boolean exists = false;
        try {
            adminClient = KafkaAdminClient.create(KafkaUtils.buildDefaultConsumerConfig(kafkaBrokerConfig));
            ListTopicsResult listTopics = adminClient.listTopics();
            exists = listTopics.names().get().containsAll(topics);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            adminClient.close();
        }
        return exists;


    }

    public List<String> getNotExistTopics(Collection<String> topics) {

        Objects.requireNonNull(topics);
        AdminClient adminClient = null;
        List<String> existsTopics = new ArrayList<>(topics);

        try {
            adminClient = KafkaAdminClient.create(KafkaUtils.buildDefaultConsumerConfig(kafkaBrokerConfig));
            ListTopicsResult listTopics = adminClient.listTopics();
            existsTopics.removeAll(listTopics.names().get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            adminClient.close();
        }
        return existsTopics;


    }

}
