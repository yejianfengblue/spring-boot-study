package com.yejianfengblue.spring.boot.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

@SpringBootTest
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
class KafkaTemplateTest {

    @Autowired
    KafkaTemplate kafkaTemplate;

    ObjectMapper objectMapper;

    private static final String TOPIC = "kafka-template-test-topic";

    private static final String GROUP = "kafka-template-test-group";

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.disable(SerializationFeature.INDENT_OUTPUT);
    }

    @SneakyThrows
    @Test
    void sendMessage() {

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
            }
        });

        ListenableFuture listenableFuture = kafkaTemplate.send(MessageBuilder
                .withPayload("Hello at " + LocalDateTime.now().toString())
                .setHeader(KafkaHeaders.TOPIC, TOPIC)
                .setHeader(KafkaHeaders.GROUP_ID, GROUP)
                .setHeader(KafkaHeaders.MESSAGE_KEY, "1")
                .build());

        listenableFuture.get(1, TimeUnit.MINUTES);
    }
}
