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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig(classes = {
        ManualSpringJavaConfigKafka.TestConsumerConfig.class,
        ManualSpringJavaConfigKafka.TestProducerConfig.class
})
@Slf4j
class ManualSpringJavaConfigKafka {

    private static final String SERVER = "localhost:9092";

    private static final String TOPIC = "manual-spring-java-config-kafka-topic";

    private static final String GROUP = "manual-spring-java-config-kafka-group";

    private static final CountDownLatch WAIT_MESSAGE_RECEIVED = new CountDownLatch(3);

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Test
    @SneakyThrows
    void manuallyConfigureKafkaConsumerAndProducer() {

        log.info("START");

        kafkaTemplate.setDefaultTopic(TOPIC);
        kafkaTemplate.sendDefault("a", "A");
        kafkaTemplate.sendDefault("b", "B");
        kafkaTemplate.sendDefault("c", "C");
        kafkaTemplate.flush();
        log.info("flush");

        kafkaTemplate.getProducerFactory().reset();
        log.info("Close existing producers");

        assertThat(WAIT_MESSAGE_RECEIVED.await(1, TimeUnit.MINUTES)).isTrue();

        TimeUnit.SECONDS.sleep(10);

        log.info("END");
    }

    // Consumer config
    /**
     * {@link EnableKafka} enables detection of KafkaListener annotations on any Spring-managed bean in the container.
     *
     * The container factory ({@link KafkaListenerContainerFactory}) to use is identified by the containerFactory
     * attribute defining the name of the {@link KafkaListenerContainerFactory} bean to use.
     * When none is set a {@link KafkaListenerContainerFactory} bean with name {@code kafkaListenerContainerFactory}
     * is assumed to be present.
     *
     * Note that the created containers are not registered with the application context but can be easily located
     * for management purposes using the
     * {@link org.springframework.kafka.config.KafkaListenerEndpointRegistry KafkaListenerEndpointRegistry}.
     */
    @Configuration
    @EnableKafka
    static class TestConsumerConfig {

        @Bean
        ConsumerFactory<String, String> consumerFactory() {

            DefaultKafkaConsumerFactory<String, String> consumerFactory =
                    new DefaultKafkaConsumerFactory<>(consumerProps());
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
         * A {@link KafkaListenerContainerFactory} bean with name {@code kafkaListenerContainerFactory} is picked up by default.
         */
        @Bean
        ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {

            ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            listenerContainerFactory.setConsumerFactory(consumerFactory);

            return listenerContainerFactory;
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
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            return props;
        }

        @Bean
        Listener listener() {
            return new Listener();
        }

        class Listener {

            @KafkaListener(topics = TOPIC, groupId = GROUP)
            public void onMessage(ConsumerRecord<String, String> message) {

                log.info("Receive : {}", message);
                WAIT_MESSAGE_RECEIVED.countDown();
            }
        }
    }

    // Producer config
    @Configuration
    static class TestProducerConfig {

        @Bean
        KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {

            KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);

            return kafkaTemplate;
        }

        @Bean
        ProducerFactory<String, String> producerFactory() {

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

            return producerFactory;
        }

        /**
         * Kafka producer properties such as server address, group ID, key value serializer.
         * Keys are defined in {@link ProducerConfig}.
         */
        private Map<String, Object> producerProps() {

            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            return props;
        }
    }
}
