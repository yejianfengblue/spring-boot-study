package com.yejianfengblue.spring.boot.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
class DetectListenerContainerIdle {

    private static final CountDownLatch MESSAGE_RECEIVED = new CountDownLatch(1);

    private static final CountDownLatch IDLE_EVENT_RECEIVED = new CountDownLatch(2);

    private static final String TEST = "detect-listener-container-idle";

    private static final String LISTENER_CONTAINER_ID = TEST;

    private static final String TOPIC = TEST + ".topic";

    private static final String GROUP = TEST + ".group";

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    ObjectMapper objectMapper;


    @BeforeEach
    void configObjectMapper() {
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @BeforeEach
    void waitForListenerContainerToStart() {
        ContainerTestUtils.waitForAssignment(
                kafkaListenerEndpointRegistry.getListenerContainer(LISTENER_CONTAINER_ID),
                1);
    }

    @Test
    @SneakyThrows
    void whenSendToTopic_thenKafkaListenerIsTriggered() {

        ProducerRecord producerRecord = new ProducerRecord(TOPIC, "Hello at " + LocalDateTime.now().toString());
        kafkaTemplate.send(producerRecord);
        log.info("Send {}", producerRecord);

        assertThat(this.MESSAGE_RECEIVED.await(5, TimeUnit.SECONDS)).isTrue();

        assertThat(this.IDLE_EVENT_RECEIVED.await(25, TimeUnit.SECONDS)).isTrue();
    }

    @TestConfiguration
    @EnableKafka
    @EnableAsync
    static class TestConfig {

        @KafkaListener(id = LISTENER_CONTAINER_ID, topics = TOPIC, groupId = GROUP)
        void receive(String message) {

            log.info("Record = {}", message);
            MESSAGE_RECEIVED.countDown();
        }

        @Async
        @EventListener
        void eventListener(ListenerContainerIdleEvent event) {
            log.info("ListenerContainerIdleEvent : {}", event);
            IDLE_EVENT_RECEIVED.countDown();
        }

        /**
         * Configure the listener container factory to create batch listeners
         */
        @Bean
        ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
                ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
                ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {

            ConcurrentKafkaListenerContainerFactory<Object, Object> listenerContainerFactory
                    = new ConcurrentKafkaListenerContainerFactory<>();
            // configure the listener container to publish a ListenerContainerIdleEvent when no message delivery after 10s
            listenerContainerFactory.getContainerProperties().setIdleEventInterval(10_000L);

            configurer.configure(listenerContainerFactory, kafkaConsumerFactory.getIfAvailable());

            return listenerContainerFactory;
        }

    }

}
