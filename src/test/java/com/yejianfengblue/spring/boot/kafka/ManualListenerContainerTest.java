package com.yejianfengblue.spring.boot.kafka;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.test.utils.ContainerTestUtils;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
class ManualListenerContainerTest {

    private static final String SERVER = "localhost:9092";

    @Autowired
    KafkaTemplate kafkaTemplate;

    private ConsumerFactory<String, String> createConsumerFactory() {

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps());

        consumerFactory.setBeanName("Magician");
        consumerFactory.addListener(new ConsumerFactory.Listener<String, String>() {
            @Override
            public void consumerAdded(String id, Consumer<String, String> consumer) {
                log.info("Consumer with ID {} is added : {}", id, consumer);
            }

            @Override
            public void consumerRemoved(String id, Consumer<String, String> consumer) {
                log.info("Consumer with ID {} is removed : {}", id, consumer);
            }
        });

        return consumerFactory;
    }

    /**
     * Kafka consumer properties such as server address, group ID, key value deserializer.
     * Keys are defined in {@link ConsumerConfig}.
     */
    private Map<String, Object> consumerProps() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return props;
    }

    @Test
    @SneakyThrows
    void manuallyConfigureListenerContainer() {

        final CountDownLatch messageReceived = new CountDownLatch(3);

        String test = "manually-configure-listener-container";
        String topic = test + "-topic";
        String group = test + "-group";

        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setGroupId(group);
        containerProperties.setClientId("9527");
        containerProperties.setMessageListener((MessageListener<String, String>) message -> {
            log.info("Receive : {}", message);
            messageReceived.countDown();
        });
        containerProperties.setCommitLogLevel(LogIfLevelEnabled.Level.INFO);

        KafkaMessageListenerContainer<String, String> listenerContainer =
                new KafkaMessageListenerContainer<>(createConsumerFactory(), containerProperties);
        listenerContainer.start();
        // wait for listener container to be ready before send test message
        ContainerTestUtils.waitForAssignment(listenerContainer, 1);

        kafkaTemplate.setDefaultTopic(topic);
        kafkaTemplate.sendDefault("Hello 1 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(100);
        kafkaTemplate.sendDefault("Hello 2 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(100);
        kafkaTemplate.sendDefault("Hello 3 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();

        assertThat(messageReceived.await(1, TimeUnit.MINUTES)).isTrue();

        listenerContainer.stop();
        log.info("Sleep to wait for listener container to stop");
        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    @SneakyThrows
    void defaultAckModeIsBATCH_thenCommitOffsetAfterAllRecordsReturnedByPollAreProcessed() {

        final CountDownLatch messageReceived = new CountDownLatch(9);

        String test = "default-ack-mode-is-batch";
        String topic = test + "-topic";
        String group = test + "-group";

        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setGroupId(group);
        containerProperties.setClientId("9527");
        containerProperties.setMessageListener((MessageListener<String, String>) message -> {
            log.info("Receive : {}", message);
            messageReceived.countDown();
        });
        containerProperties.setCommitLogLevel(LogIfLevelEnabled.Level.INFO);

        KafkaMessageListenerContainer<String, String> listenerContainer =
                new KafkaMessageListenerContainer<>(createConsumerFactory(), containerProperties);
        listenerContainer.start();
        // wait for listener container to be ready before send test message
        ContainerTestUtils.waitForAssignment(listenerContainer, 1);

        kafkaTemplate.setDefaultTopic(topic);
        kafkaTemplate.sendDefault("1", "Hello 1 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("2", "Hello 2 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("3", "Hello 3 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(100);
        kafkaTemplate.sendDefault("4", "Hello 4 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("5", "Hello 5 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("6", "Hello 6 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(100);
        kafkaTemplate.sendDefault("7", "Hello 7 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("8", "Hello 8 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("9", "Hello 9 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();

        assertThat(messageReceived.await(1, TimeUnit.MINUTES)).isTrue();

        listenerContainer.stop();
        log.info("Sleep to wait for listener container to stop");
        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    @SneakyThrows
    void whenAckModeIsRECORD_thenCommitOffsetAfterEachRecordIsProcessed() {

        final CountDownLatch messageReceived = new CountDownLatch(9);

        String test = "ack-mode-is-record";
        String topic = test + "-topic";
        String group = test + "-group";

        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setGroupId(group);
        containerProperties.setClientId("9527");
        containerProperties.setMessageListener((MessageListener<String, String>) message -> {
            log.info("Receive : {}", message);
            messageReceived.countDown();
        });
        containerProperties.setCommitLogLevel(LogIfLevelEnabled.Level.INFO);
        containerProperties.setAckMode(ContainerProperties.AckMode.RECORD);

        KafkaMessageListenerContainer<String, String> listenerContainer =
                new KafkaMessageListenerContainer<>(createConsumerFactory(), containerProperties);
        listenerContainer.start();
        // wait for listener container to be ready before send test message
        ContainerTestUtils.waitForAssignment(listenerContainer, 1);

        kafkaTemplate.setDefaultTopic(topic);
        kafkaTemplate.sendDefault("1", "Hello 1 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("2", "Hello 2 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("3", "Hello 3 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(100);
        kafkaTemplate.sendDefault("4", "Hello 4 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("5", "Hello 5 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("6", "Hello 6 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(100);
        kafkaTemplate.sendDefault("7", "Hello 7 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("8", "Hello 8 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("9", "Hello 9 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();

        assertThat(messageReceived.await(1, TimeUnit.MINUTES)).isTrue();

        listenerContainer.stop();
        log.info("Sleep to wait for listener container to stop");
        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    @SneakyThrows
    void whenAckModeIsCOUNT_thenCommitOffsetAfterAllRecordsReturnedByPollHaveBeenProcessedAndackCountRecordsReceivedSinceLastCommit() {

        final CountDownLatch messageReceived = new CountDownLatch(9);

        String test = "ack-mode-is-count";
        String topic = test + "-topic";
        String group = test + "-group";

        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setGroupId(group);
        containerProperties.setClientId("9527");
        containerProperties.setMessageListener((MessageListener<String, String>) message -> {
            log.info("Receive : {}", message);
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {}
            messageReceived.countDown();
        });
        containerProperties.setCommitLogLevel(LogIfLevelEnabled.Level.INFO);
        containerProperties.setAckMode(ContainerProperties.AckMode.COUNT);
        containerProperties.setAckCount(4);

        KafkaMessageListenerContainer<String, String> listenerContainer =
                new KafkaMessageListenerContainer<>(createConsumerFactory(), containerProperties);
        listenerContainer.start();
        // wait for listener container to be ready before send test message
        ContainerTestUtils.waitForAssignment(listenerContainer, 1);

        kafkaTemplate.setDefaultTopic(topic);
        kafkaTemplate.sendDefault("1", "Hello 1 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("2", "Hello 2 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("3", "Hello 3 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(500);
        kafkaTemplate.sendDefault("4", "Hello 4 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("5", "Hello 5 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("6", "Hello 6 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(500);
        kafkaTemplate.sendDefault("7", "Hello 7 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("8", "Hello 8 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("9", "Hello 9 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();

        assertThat(messageReceived.await(1, TimeUnit.MINUTES)).isTrue();

        listenerContainer.stop();
        log.info("Sleep to wait for listener container to stop");
        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    @SneakyThrows
    void whenAckModeIsMANUAL_thenCommitOffset() {

        final CountDownLatch messageReceived = new CountDownLatch(9);

        String test = "ack-mode-is-manual";
        String topic = test + "-topic";
        String group = test + "-group";

        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setGroupId(group);
        containerProperties.setClientId("9527");
        containerProperties.setMessageListener((AcknowledgingMessageListener<String, String>) (message, acknowledgment) -> {
            log.info("Receive : {}", message);
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {}
            if (Integer.valueOf(message.key()) % 3 == 0) {
                log.info("key = {}, nack", message.key());
                acknowledgment.nack(100);
            } else {
                log.info("key = {}, ack", message.key());
                acknowledgment.acknowledge();
            }
            messageReceived.countDown();
        });
        containerProperties.setCommitLogLevel(LogIfLevelEnabled.Level.INFO);
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);

        KafkaMessageListenerContainer<String, String> listenerContainer =
                new KafkaMessageListenerContainer<>(createConsumerFactory(), containerProperties);
        listenerContainer.start();
        // wait for listener container to be ready before send test message
        ContainerTestUtils.waitForAssignment(listenerContainer, 1);

        kafkaTemplate.setDefaultTopic(topic);
        kafkaTemplate.sendDefault("1", "Hello 1 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("2", "Hello 2 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("3", "Hello 3 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(500);
        kafkaTemplate.sendDefault("4", "Hello 4 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("5", "Hello 5 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("6", "Hello 6 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(500);
        kafkaTemplate.sendDefault("7", "Hello 7 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("8", "Hello 8 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("9", "Hello 9 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();

        assertThat(messageReceived.await(1, TimeUnit.MINUTES)).isTrue();

        listenerContainer.stop();
        log.info("Sleep to wait for listener container to stop");
        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    @SneakyThrows
    void whenAckModeIsMANUAL_IMMEDIATE_thenCommitOffset() {

        final CountDownLatch messageReceived = new CountDownLatch(9);

        String test = "ack-mode-is-manual-immediate";
        String topic = test + "-topic";
        String group = test + "-group";

        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setGroupId(group);
        containerProperties.setClientId("9527");
        containerProperties.setMessageListener((AcknowledgingMessageListener<String, String>) (message, acknowledgment) -> {
            log.info("Receive : {}", message);
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {}
            if (Integer.valueOf(message.key()) % 3 == 0) {
                log.info("key = {}, nack", message.key());
                acknowledgment.nack(100);
            } else {
                log.info("key = {}, ack", message.key());
                acknowledgment.acknowledge();
            }
            messageReceived.countDown();
        });
        containerProperties.setCommitLogLevel(LogIfLevelEnabled.Level.INFO);
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        KafkaMessageListenerContainer<String, String> listenerContainer =
                new KafkaMessageListenerContainer<>(createConsumerFactory(), containerProperties);
        listenerContainer.start();
        // wait for listener container to be ready before send test message
        ContainerTestUtils.waitForAssignment(listenerContainer, 1);

        kafkaTemplate.setDefaultTopic(topic);
        kafkaTemplate.sendDefault("1", "Hello 1 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("2", "Hello 2 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("3", "Hello 3 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(500);
        kafkaTemplate.sendDefault("4", "Hello 4 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("5", "Hello 5 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("6", "Hello 6 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(500);
        kafkaTemplate.sendDefault("7", "Hello 7 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("8", "Hello 8 at " + LocalDateTime.now().toString());
        kafkaTemplate.sendDefault("9", "Hello 9 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();

        assertThat(messageReceived.await(1, TimeUnit.MINUTES)).isTrue();

        listenerContainer.stop();
        log.info("Sleep to wait for listener container to stop");
        TimeUnit.SECONDS.sleep(1);
    }
}
