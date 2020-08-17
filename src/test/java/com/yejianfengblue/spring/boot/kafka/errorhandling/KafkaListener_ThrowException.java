package com.yejianfengblue.spring.boot.kafka.errorhandling;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "topic = throw-exception-topic",
        "group = throw-exception-group"
})
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class
})
@Slf4j
class KafkaListener_ThrowException {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Value("${topic}")
    private String topic;

    private static final CountDownLatch WAIT_MESSAGE_RECEIVED = new CountDownLatch(3);

    @TestConfiguration
    @EnableKafka
    static class TestKafkaConfig {

        @KafkaListener(
                topics = "${topic}",
                groupId = "${group}")
        void receive(String message) {

            log.info("Receive : {}", message);

            WAIT_MESSAGE_RECEIVED.countDown();

            if (message.length() % 2 == 0) {
                throw new RuntimeException("Failed with message length " + message.length());
            }
        }
    }

    /**
     * The exception log is thrown by {@link SeekToCurrentErrorHandler}
     */
    @Test
    @SneakyThrows
    void whenKafkaListenerThrowException_thenExceptionIsLoggedByDefaultAndRetry() {

        kafkaTemplate.send(topic, "1");
        kafkaTemplate.send(topic, "23");
        kafkaTemplate.send(topic, "4");

        assertThat(WAIT_MESSAGE_RECEIVED.await(30, TimeUnit.SECONDS)).isTrue();
    }
}
