package com.yejianfengblue.spring.boot.rabbitmq;

import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.TimeUnit;

/**
 * A Rabbit MQ example where the Rabbit MQ server is running on a remote host,
 * one sender publishes "hello world" and one receiver consumes message.
 * Set the spring.rabbitmq.* properties in @SpringBootTest.properties before start the two tests.
 * Run SenderTest first and then ReceiverTest
 * @author yejianfengblue
 */
class RemoteHelloTest {

    private static final String QUEUE_NAME = "spring-boot-study-rabbitmq-remote-hello-test-queue";

    // with @SpringBootTest (@EnableAutoConfiguration) and amqp dependency in classpath, RabbitAutoConfiguration is evaluated to be matched
    @SpringBootTest(properties = {
            "spring.rabbitmq.host = 192.168.31.201",
            "spring.rabbitmq.port = 5672",
            "spring.rabbitmq.username = hello",
            "spring.rabbitmq.password = hello"
    })
    // don't create the test database
    @EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
    static class SenderTest {

        @Test
        void testSender() {

            // sleep a while to wait for the scheduled rabbitmq sender to send some message.
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @TestConfiguration  // Nested @TestConfiguration will be picked up by @SpringBootTest
        @EnableScheduling
        static class SchedulingConfig {}

         @TestConfiguration  // Nested @TestConfiguration will be picked up by @SpringBootTest
        static class RabbitmqSenderConfig {

            // There is no way to define test-specific component, so use a @TestConfiguration to define @Bean
            @Bean
            RabbitmqSender rabbitmqSender(RabbitTemplate rabbitTemplate) {
                return new RabbitmqSender(rabbitTemplate);
            }
        }

        @RequiredArgsConstructor
        static class RabbitmqSender {

            private final RabbitTemplate rabbitTemplate;

            private Logger log = LoggerFactory.getLogger(getClass());

            @Scheduled(fixedDelay = 1000, initialDelay = 1000)
            public void send() {

                String message = "Hello!!!";
                // send to the no-name exchange (AMQP default) with provided routingKey
                rabbitTemplate.convertAndSend(QUEUE_NAME, message);
                log.info("Send '{}'", message);
            }
        }
    }

    // with @SpringBootTest (@EnableAutoConfiguration) and amqp dependency in classpath,
    // RabbitAutoConfiguration is evaluated to be matched, and then auto @EnableRabbit
    @SpringBootTest(properties = {
            "spring.rabbitmq.host = 192.168.31.201",
            "spring.rabbitmq.port = 5672",
            "spring.rabbitmq.username = hello",
            "spring.rabbitmq.password = hello"
    })
    @EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
    static class ReceiverTest {

        @TestConfiguration
        static class RabbitmqReceiverConfig {

            @Bean
            RabbitmqReceiver rabbitmqReceiver() {
                return new RabbitmqReceiver();
            }

            @Bean
            Queue queue() {
                // a named queue is bound to no-name exchange (AMQP default) by a fixed routing key (the queue's name)
                return new Queue(QUEUE_NAME, false, false, true);
            }
        }

        static class RabbitmqReceiver {

            static final String receiverId = "hello-receiver";

            private Logger log = LoggerFactory.getLogger(getClass());

            @RabbitListener(id = receiverId, queues = QUEUE_NAME)
            void receive(String message) {
                log.info("Receive '{}'", message);
            }
        }

        @Test
        void testReceiver() {

            // sleep a while to wait for rabbit listener to consume message
            try {
                TimeUnit.SECONDS.sleep(60);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
