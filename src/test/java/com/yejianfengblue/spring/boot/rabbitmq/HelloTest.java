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
 * A Rabbit MQ example where one sender publishes "hello world" and one receiver consumes message.
 * Run a RabbitMQ server on localhost:5672 with default user guest:guest before start the two tests.
 * Run SenderTest first and then ReceiverTest
 * @author yejianfengblue
 */
class HelloTest {

    private static final String QUEUE_NAME = "spring-boot-study-rabbitmq-hello-test-queue";

    // with @SpringBootTest (@EnableAutoConfiguration) and amqp dependency in classpath, RabbitAutoConfiguration is evaluated to be matched
    @SpringBootTest
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
    @SpringBootTest
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
                return new Queue(QUEUE_NAME);
            }
        }

        static class RabbitmqReceiver {

            private Logger log = LoggerFactory.getLogger(getClass());

            @RabbitListener(queues = QUEUE_NAME)
            void receive(String message) {
                log.info("Receive '{}'", message);
            }
        }

        @Test
        void testReceiver() {

            // sleep a while to wait for rabbit listener to consume message
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
