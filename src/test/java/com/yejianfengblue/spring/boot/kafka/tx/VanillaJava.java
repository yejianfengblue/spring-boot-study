package com.yejianfengblue.spring.boot.kafka.tx;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

class VanillaJava {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    private static final String TOPIC = "tx-vanilla-java-topic";

    @Test
    void transactionAwareProducer() {

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        // enable idempotence, Kafka use transaction id as part of its algorithm to deduplicate message, similar to TCP
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // make sure the transaction id is distinct for each producer, though consistent across restarts
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionAwareProducer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);

        producer.initTransactions();


    }

    @Test
    void transactionAwareConsumer() {

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "transactionAwareConsumerGroup");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // default isolation level is read_uncommitted
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString());

        KafkaConsumer consumer = new KafkaConsumer(consumerProps);
        consumer.subscribe(List.of(TOPIC));


    }
}


















