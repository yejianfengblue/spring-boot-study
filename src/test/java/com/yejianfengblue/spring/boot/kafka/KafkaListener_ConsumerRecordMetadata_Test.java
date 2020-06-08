package com.yejianfengblue.spring.boot.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.time.Instant;
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
class KafkaListener_ConsumerRecordMetadata_Test {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    private static final CountDownLatch latch = new CountDownLatch(1);

    private static final String TOPIC = "consumer-record-metadata-test-topic";

    private static final String GROUP = "consumer-record-metadata-test-group";

    @BeforeEach
    void configObjectMapper() {
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Test
    @SneakyThrows
    void whenSendToTopic_thenKafkaListenerIsTriggered() {

        ProducerRecord producerRecord = new ProducerRecord(TOPIC, "Hello at " + LocalDateTime.now().toString());
        kafkaTemplate.send(producerRecord);

        assertThat(producerRecord.topic()).isEqualToIgnoringCase(TOPIC);
        assertThat(producerRecord.partition()).isNull();
        assertThat(producerRecord.key()).isNull();
        log.info("producerRecord : {}", producerRecord);

        assertThat(this.latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @TestConfiguration
    @EnableKafka
    static class TestKafkaConfig {

        @KafkaListener(topics = TOPIC, groupId = GROUP)
        void receive(@Payload String value,
                     @Header(KafkaHeaders.GROUP_ID) String groupId,
                     ConsumerRecordMetadata metadata,
                     @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                     @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition,
                     @Header(KafkaHeaders.TIMESTAMP_TYPE) TimestampType timestampType,
                     @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp,
                     @Header(name = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) String key,
                     @Header(KafkaHeaders.OFFSET) Long offset) {

            latch.countDown();

            assertThat(groupId).isEqualTo("consumer-record-metadata-test-group");
            assertThat(metadata.topic()).isEqualTo(TOPIC);
            assertThat(metadata.partition()).isEqualTo(partition);
            assertThat(metadata.hasTimestamp()).isTrue();
            assertThat(metadata.timestampType()).isEqualTo(timestampType);
            assertThat(metadata.timestamp()).isEqualTo(timestamp);
            assertThat(metadata.hasOffset()).isTrue();
            assertThat(metadata.offset()).isEqualTo(offset);
            log.info("metadata.keySize : {}", metadata.serializedKeySize());
            log.info("metadata.valueSize : {}", metadata.serializedValueSize());

            log.info("value : {}", value);

            log.info("topic : {}", topic);
            log.info("partition : {}", partition);
            log.info("timestampType : {}", timestampType);
            log.info("timestamp : {}", Instant.ofEpochMilli(timestamp));
            log.info("key : {}", key);
            log.info("offset : {}", offset);

        }

    }

}
