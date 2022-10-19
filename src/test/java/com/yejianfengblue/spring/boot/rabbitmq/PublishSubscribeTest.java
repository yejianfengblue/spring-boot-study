package com.yejianfengblue.spring.boot.rabbitmq;

import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.amqp.rabbit.test.RabbitListenerTestHarness;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.inOrder;

/**
 * A Rabbit MQ example implementing the publish/subscribe pattern. One publisher and two subscribers "Apple" and "Banana".
 * <ul>
 * <li>Run a RabbitMQ server on localhost:5672 with default user guest:guest before start the two tests.
 * <li>Run method test() in the two subscribers first and then publisher.
 * <li>If an exchange named {@link #EXCHANGE_NAME} already exists but it's not "durable and auto-delete",
 * delete it first before run subscriber, because the exchange is declared to be durable and auto-delete in the
 * two Subscribers and exchanges with same name but diff features (durable and auto-delete) are mutually exclusive.
 * </ul>
 * @author yejianfengblue
 */
class PublishSubscribeTest {

    private static final String EXCHANGE_NAME = "spring-boot-study-rabbitmq-publish-subscribe-test-exchange";

    @SpringBootTest
    @EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
    static class PublisherTest {

        @Autowired
        private RabbitTemplate rabbitTemplate;

        private Logger log = LoggerFactory.getLogger(getClass());

        @TestConfiguration
        @EnableScheduling
        static class SchedulingConfig {}

        @TestConfiguration
        static class PublisherConfig {

            @Bean
            Publisher publisher(RabbitTemplate rabbitTemplate) {

                rabbitTemplate.setExchange(EXCHANGE_NAME);
                return new Publisher(rabbitTemplate);
            }
        }

        @RequiredArgsConstructor
        static class Publisher {

            private final RabbitTemplate rabbitTemplate;

            private AtomicInteger atomicInteger = new AtomicInteger(0);

            private Logger log = LoggerFactory.getLogger(getClass());

            @Scheduled(fixedDelay = 1000, initialDelay = 1000)
            void publish() {
                // routing key is ignored in the case of fanout exchange
                rabbitTemplate.convertAndSend(EXCHANGE_NAME, "2", atomicInteger.getAndIncrement());
            }
        }

        @Test
        void test() throws InterruptedException {

            TimeUnit.SECONDS.sleep(60);
        }
    }

    @SpringBootTest
    @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
    static class SubscriberApple {

        static class Subscriber {

            static final String receiverId = "subscriber-apple";

            private Logger log = LoggerFactory.getLogger(getClass());

            @RabbitListener(id = receiverId, queues = "#{queue.name}")
            void subscribe(String message) {
                log.info("Receive '{}'", message);
            }
        }

        @TestConfiguration
        static class RabbitConfig {

            @Bean
            Subscriber subscriber() {
                return new Subscriber();
            }

            // create a named, durable, auto-deleted exchange in RabbitMQ server, if the named exchange doesn't exist
            @Bean
            FanoutExchange fanout() {
                return new FanoutExchange(EXCHANGE_NAME, true, true);
            }

            @Bean
            Queue queue() {
                return new AnonymousQueue();
            }

            @Bean
            Binding binding(FanoutExchange fanoutExchange, Queue queue) {
                return BindingBuilder.bind(queue).to(fanoutExchange);
            }
        }

        @Test
        void test() throws InterruptedException {

            TimeUnit.SECONDS.sleep(60);
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
        void givenSubscribingFanoutExchange_whenSendMessageToFanoutExchangeInOrder_thenReceiveMessageInOrder() {

            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "", "I am the bone of my sword");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "", "Steel is my body, and fire is my blood");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "", "I have created over a thousand blades");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "", "Unknown to Death, nor known to Life");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "", "Have withstood pain to create many weapons");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "", "Yet, those hands will never hold anything");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "", "So as I pray, Unlimited Blade Works");

            Subscriber subscriber = rabbitListenerTestHarness.getSpy(Subscriber.receiverId);
            assertNotNull(subscriber);
            InOrder inOrder = inOrder(subscriber);
            inOrder.verify(subscriber).subscribe("I am the bone of my sword");
            inOrder.verify(subscriber).subscribe("Steel is my body, and fire is my blood");
            inOrder.verify(subscriber).subscribe("I have created over a thousand blades");
            inOrder.verify(subscriber).subscribe("Unknown to Death, nor known to Life");
            inOrder.verify(subscriber).subscribe("Have withstood pain to create many weapons");
            inOrder.verify(subscriber).subscribe("Yet, those hands will never hold anything");
            inOrder.verify(subscriber).subscribe("So as I pray, Unlimited Blade Works");
        }
    }

    @SpringBootTest
    @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
    static class SubscriberBanana {

        static class Subscriber {

            static final String receiverId = "subscriber-banana";

            private Logger log = LoggerFactory.getLogger(getClass());

            @RabbitListener(id = receiverId, queues = "#{queue.name}")
            void subscribe(String message) {
                log.info("Receive '{}'", message);
            }
        }

        @TestConfiguration
        static class RabbitConfig {

            @Bean
            Subscriber subscriber() {
                return new Subscriber();
            }

            // create a named, durable, auto-deleted exchange in RabbitMQ server, if the named exchange doesn't exist
            @Bean
            FanoutExchange fanout() {
                return new FanoutExchange(EXCHANGE_NAME, true, true);
            }

            @Bean
            Queue queue() {
                return new AnonymousQueue();
            }

            @Bean
            Binding binding(FanoutExchange fanoutExchange, Queue queue) {
                return BindingBuilder.bind(queue).to(fanoutExchange);
            }
        }

        @Test
        void test() throws InterruptedException {

            TimeUnit.SECONDS.sleep(60);
        }
    }
}
