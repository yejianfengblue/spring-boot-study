package com.yejianfengblue.spring.boot.rabbitmq;

import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import org.mockito.BDDMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.amqp.rabbit.test.RabbitListenerTestHarness;
import org.springframework.amqp.rabbit.test.mockito.LatchCountDownAndCallRealMethodAnswer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

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
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @TestConfiguration
        @RabbitListenerTest
        static class RabbitListenerTestConfig {}

        @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
        @Autowired
        private RabbitListenerTestHarness rabbitListenerTestHarness;

        @Autowired
        private RabbitTemplate rabbitTemplate;

        @Test
        void givenRabbitListenerListeningOnQueue_whenSendToThatQueue_thenListenerMethodIsCalled() {

            // convertSendAndReceive suspend the test thread, ensure @RabbitListener method is called
            rabbitTemplate.convertSendAndReceive(QUEUE_NAME, "Hail Hydra");

            RabbitmqReceiver rabbitmqReceiver = rabbitListenerTestHarness.getSpy(RabbitmqReceiver.receiverId);
            assertNotNull(rabbitmqReceiver);
            verify(rabbitmqReceiver).receive("Hail Hydra");
        }

        @Test
        void givenLatchCountDownAndCallRealMethodAnswer_whenSendToQueue_thenAnswerCountDown() throws InterruptedException {


            RabbitmqReceiver rabbitmqReceiver = rabbitListenerTestHarness.getSpy(RabbitmqReceiver.receiverId);
            LatchCountDownAndCallRealMethodAnswer answer = new LatchCountDownAndCallRealMethodAnswer(2);
            // given
            BDDMockito.doAnswer(answer).when(rabbitmqReceiver).receive(anyString());  // use doAnswer() coz receive() return void

            // when
            rabbitTemplate.convertAndSend(QUEUE_NAME, "I am the bone of my sword");
            rabbitTemplate.convertAndSend(QUEUE_NAME, "Steel is my body, and fire is my blood");

            // then
            assertTrue(answer.getLatch().await(10, TimeUnit.SECONDS));
            BDDMockito.then(rabbitmqReceiver).should().receive("I am the bone of my sword");
            BDDMockito.then(rabbitmqReceiver).should().receive("Steel is my body, and fire is my blood");
        }

    }
}
