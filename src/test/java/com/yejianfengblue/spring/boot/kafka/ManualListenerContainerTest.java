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

    private static final String TOPIC = "manual-listener-container-topic";

    private static final String GROUP = "manual-listener-container-group";

    private static final CountDownLatch WAIT_MESSAGE_RECEIVED = new CountDownLatch(3);

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

        ContainerProperties containerProperties = new ContainerProperties(TOPIC);
        containerProperties.setGroupId(GROUP);
        containerProperties.setClientId("9527");
        containerProperties.setMessageListener((MessageListener<String, String>) message -> {
            log.info("Receive : {}", message);
            WAIT_MESSAGE_RECEIVED.countDown();
        });
        containerProperties.setCommitLogLevel(LogIfLevelEnabled.Level.INFO);

        KafkaMessageListenerContainer<String, String> listenerContainer =
                new KafkaMessageListenerContainer<>(createConsumerFactory(), containerProperties);
        listenerContainer.start();
        // wait for listener container to be ready before send test message
        ContainerTestUtils.waitForAssignment(listenerContainer, 1);

        kafkaTemplate.setDefaultTopic(TOPIC);
        kafkaTemplate.sendDefault("Hello 1 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(100);
        kafkaTemplate.sendDefault("Hello 2 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();
        TimeUnit.MILLISECONDS.sleep(100);
        kafkaTemplate.sendDefault("Hello 3 at " + LocalDateTime.now().toString());
        kafkaTemplate.flush();

        assertThat(WAIT_MESSAGE_RECEIVED.await(1, TimeUnit.MINUTES)).isTrue();

        listenerContainer.stop();
        log.info("Sleep to wait for listener container to stop");
        TimeUnit.SECONDS.sleep(1);
    }

}
