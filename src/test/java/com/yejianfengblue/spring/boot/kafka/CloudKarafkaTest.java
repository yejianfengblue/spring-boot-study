package com.yejianfengblue.spring.boot.kafka;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        properties = {
                "spring.kafka.bootstrap-servers=moped-03.srvs.cloudkafka.com:9094,moped-02.srvs.cloudkafka.com:9094,moped-01.srvs.cloudkafka.com:9094",
                "spring.kafka.security.protocol=SASL_SSL",
                "spring.kafka.properties.sasl.mechanism=SCRAM-SHA-256",
                "spring.kafka.jaas.enabled=true",
                "spring.kafka.jaas.login-module=org.apache.kafka.common.security.scram.ScramLoginModule",
                "spring.kafka.jaas.control-flag=required",
                "spring.kafka.jaas.options.username=2g7x91l0",
                "spring.kafka.jaas.options.password=Ld9gFN1ckwuq9vATANhZA8anhM9YZKlE",
                "spring.kafka.consumer.group-id=2g7x91l0-consumers",
                "spring.kafka.consumer.auto-offset-reset=earliest",
                "cloudkarafka.topic=2g7x91l0-default"
        }
)
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
public class CloudKarafkaTest {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Value("${cloudkarafka.topic}")
    private String topic;

    private static final CountDownLatch latch = new CountDownLatch(1);

    @Test
    @SneakyThrows
    void whenSendToTopic_thenKafkaListenerIsTriggered() {

        ProducerRecord producerRecord = new ProducerRecord(topic, "Hello at " + LocalDateTime.now().toString());
        kafkaTemplate.send(producerRecord);

        assertThat(producerRecord.topic()).isEqualToIgnoringCase(topic);
        log.info(producerRecord.toString());

        assertThat(this.latch.await(60, TimeUnit.SECONDS)).isTrue();
    }

    @TestConfiguration
    @EnableKafka
    static class TestKafkaConfig {

        @KafkaListener(topics = "${cloudkarafka.topic}")
        void receive(ConsumerRecord<?, ?> consumerRecord) {

            latch.countDown();

            log.info("consumerRecord : {}", consumerRecord.toString());
        }

    }
}
