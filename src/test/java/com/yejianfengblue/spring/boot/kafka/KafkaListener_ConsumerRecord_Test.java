package com.yejianfengblue.spring.boot.kafka;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

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
class KafkaListener_ConsumerRecord_Test {

    @Autowired
    KafkaTemplate kafkaTemplate;

    private static final CountDownLatch latch = new CountDownLatch(1);

    private static final String TOPIC = "consumer-record-test-topic";

    private static final String GROUP = "consumer-record-test-group";

    @Test
    @SneakyThrows
    void whenSendToTopic_thenKafkaListenerIsTriggered() {

        ProducerRecord producerRecord = new ProducerRecord(TOPIC, "Hello at " + LocalDateTime.now().toString());
        kafkaTemplate.send(producerRecord);

        assertThat(producerRecord.topic()).isEqualToIgnoringCase(TOPIC);
        assertThat(producerRecord.partition()).isNull();
        assertThat(producerRecord.key()).isNull();
        log.info(producerRecord.toString());

        assertThat(this.latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Configuration
    @EnableKafka
    static class TestKafkaConfig {

        @KafkaListener(topics = TOPIC, groupId = GROUP)
        void receive(ConsumerRecord<?, ?> consumerRecord) {

            latch.countDown();

            assertThat(consumerRecord.topic()).isEqualTo(TOPIC);

            log.info("consumerRecord : {}", consumerRecord.toString());
        }

    }

}
