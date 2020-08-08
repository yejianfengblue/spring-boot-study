package com.yejianfengblue.spring.boot.kafka;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class
})
@Slf4j
class ManualConfigKafka {

    private static final String SERVER = "localhost:9092";

    private static final String TOPIC = "manual-config-kafka-topic";

    private final CountDownLatch waitMessageReceived = new CountDownLatch(3);

    @Test
    @SneakyThrows
    void manuallyConfigureKafkaConsumerAndProducer() {

        // configure consumer side
        KafkaMessageListenerContainer<String, String> listenerContainer = createContainer();
        listenerContainer.start();
        log.info("Listener START");

        // configure producer side
        KafkaTemplate<String, String> kafkaTemplate = createTemplate();
        kafkaTemplate.sendDefault("a", "A");
        kafkaTemplate.sendDefault("b", "B");
        kafkaTemplate.sendDefault("c", "C");
        kafkaTemplate.flush();
        log.info("flush");

        kafkaTemplate.getProducerFactory().reset();
        log.info("Close existing producers");

        assertThat(waitMessageReceived.await(1, TimeUnit.MINUTES)).isTrue();

        listenerContainer.stop();
        log.info("Listener STOP");

        TimeUnit.SECONDS.sleep(10);
    }

    // Consumer config

    private KafkaMessageListenerContainer<String, String> createContainer() {

        DefaultKafkaConsumerFactory<String, String> kafkaConsumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps());
        kafkaConsumerFactory.addListener(new ConsumerFactory.Listener<String, String>() {
            @Override
            public void consumerAdded(String id, Consumer<String, String> consumer) {
                log.info("Consumer with ID {} is added : {}", id, consumer);
            }

            @Override
            public void consumerRemoved(String id, Consumer<String, String> consumer) {
                log.info("Consumer with ID {} is removed : {}", id, consumer);
            }
        });

        // Single-threaded message listener container
        KafkaMessageListenerContainer<String, String> listenerContainer =
                new KafkaMessageListenerContainer<>(kafkaConsumerFactory, containerProps());

        return listenerContainer;
    }

    /**
     * Kafka consumer properties such as server address, group ID, key value deserializer.
     * Keys are defined in {@link ConsumerConfig}.
     * @return
     */
    private Map<String, Object> consumerProps() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    /**
     * runtime properties for a listener container
     * @return
     */
    private ContainerProperties containerProps() {

        ContainerProperties containerProperties = new ContainerProperties(TOPIC);
        // group ID can be set in container also
//        containerProperties.setGroupId(UUID.randomUUID().toString());
        containerProperties.setMessageListener(new MessageListener<String, String>() {
            @Override
            public void onMessage(ConsumerRecord<String, String> message) {

                log.info("Receive : {}", message);
                waitMessageReceived.countDown();
            }
        });

        return containerProperties;
    }

    // Producer config

    private KafkaTemplate<String, String> createTemplate() {

        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps());
        producerFactory.addListener(new ProducerFactory.Listener<String, String>() {
            @Override
            public void producerAdded(String id, Producer<String, String> producer) {
                log.info("Producer with ID {} is added : {}", id, producer);
            }

            @Override
            public void producerRemoved(String id, Producer<String, String> producer) {
                log.info("Producer with ID {} is removed : {}", id, producer);
            }
        });
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setDefaultTopic(TOPIC);

        return kafkaTemplate;
    }

    /**
     * Kafka producer properties such as server address, group ID, key value serializer.
     * Keys are defined in {@link ProducerConfig}.
     * @return
     */
    private Map<String, Object> producerProps() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return props;
    }
}
