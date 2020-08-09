package com.yejianfengblue.spring.boot.kafka;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
class KafkaTemplateTest {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    ProducerFactory producerFactory;

    ObjectMapper objectMapper;

    private static final String TOPIC = "kafka-template-test-topic";

    private static final String GROUP = "kafka-template-test-group";

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.disable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    /**
     * Make sure a Kafka broker is running on localhost:9092 before running this test
     */
    @SneakyThrows
    @Test
    void whenSendSuccessfully_thenProducerListenerOnSuccessIsInvoked() {

        CountDownLatch success = new CountDownLatch(1);

        kafkaTemplate.setProducerListener(new ProducerListener() {
            @SneakyThrows
            @Override
            public void onSuccess(ProducerRecord producerRecord, RecordMetadata recordMetadata) {
                log.info("Success. producerRecord = {}, recordMetadata = {}",
                        objectMapper.writeValueAsString(producerRecord),
                        objectMapper.writeValueAsString(recordMetadata));
                success.countDown();
            }

            @SneakyThrows
            @Override
            public void onError(ProducerRecord producerRecord, Exception exception) {
                log.error("Error. producerRecord = {}, exception = {}",
                        objectMapper.writeValueAsString(producerRecord),
                        objectMapper.writeValueAsString(exception.getMessage()));
            }
        });

        ListenableFuture listenableFuture = kafkaTemplate.send(MessageBuilder
                .withPayload("Hello at " + LocalDateTime.now().toString())
                .setHeader(KafkaHeaders.TOPIC, TOPIC)
                .setHeader(KafkaHeaders.GROUP_ID, GROUP)
                .setHeader(KafkaHeaders.MESSAGE_KEY, "1")
                .build());

        assertThat(success.await(10, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * Make sure no Kafka broker is running on localhost:55555 before running this test
     */
    @SneakyThrows
    @Test
    void whenSendFailed_thenProducerListenerOnErrorIsInvoked() {

        CountDownLatch error = new CountDownLatch(1);

        int timeout = 5_000;

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:55555");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate kafkaTemplate = new KafkaTemplate(producerFactory,
                Map.of("max.block.ms", timeout));

        kafkaTemplate.setProducerListener(new ProducerListener() {
            @SneakyThrows
            @Override
            public void onSuccess(ProducerRecord producerRecord, RecordMetadata recordMetadata) {
                log.info("Success. producerRecord = {}, recordMetadata = {}",
                        objectMapper.writeValueAsString(producerRecord),
                        objectMapper.writeValueAsString(recordMetadata));
            }

            @SneakyThrows
            @Override
            public void onError(ProducerRecord producerRecord, Exception exception) {
                log.error("Error. producerRecord = {}, exception = {}",
                        objectMapper.writeValueAsString(producerRecord),
                        objectMapper.writeValueAsString(exception.getMessage()));
                error.countDown();
            }
        });

        assertThatThrownBy(() ->
            kafkaTemplate.send(MessageBuilder
                    .withPayload("Hello at " + LocalDateTime.now().toString())
                    .setHeader(KafkaHeaders.TOPIC, TOPIC)
                    .setHeader(KafkaHeaders.GROUP_ID, GROUP)
                    .setHeader(KafkaHeaders.MESSAGE_KEY, "1")
                    .build()))
                .isInstanceOf(KafkaException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasMessageContaining(String.format("Topic %s not present in metadata after %d ms.", TOPIC, timeout));

        assertThat(error.await(20, TimeUnit.SECONDS)).isTrue();
    }
}
