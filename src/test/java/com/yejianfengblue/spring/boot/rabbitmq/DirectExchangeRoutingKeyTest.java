package com.yejianfengblue.spring.boot.rabbitmq;

import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import org.mockito.BDDMockito;
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

/**
 * A Rabbit MQ example where sender send message to direct exchange with diff routingKey, and receiver is able to
 * receive message selectively by routingKey.
 * <ul>
 * <li>Run a RabbitMQ server on localhost:5672 with default user guest:guest before start the two tests.
 * <li>Run method test() in the receiver first and then sender, because receiver creates exchange if not exists.
 * <li>If an exchange named {@link #EXCHANGE_NAME} already exists but it's not "durable and auto-delete",
 * delete it first before run subscriber, because the exchange is declared to be durable and auto-delete in the
 * two Subscribers and exchanges with same name but diff features (durable and auto-delete) are mutually exclusive.
 * </ul>
 * @author yejianfengblue
 */
class DirectExchangeRoutingKeyTest {

    private static final String EXCHANGE_NAME = "spring-boot-study-rabbitmq-direct-exchange-routing-key-test-exchange";

    private enum RoutingKey { RED, GREEN, BLUE }

    @SpringBootTest
    @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
    static class SenderTest {


        @TestConfiguration
        @EnableScheduling
        static class SchedulingConfig {}

        @TestConfiguration
        static class SenderConfig {

            @Bean
            Sender sender(RabbitTemplate rabbitTemplate) {
                return new Sender(rabbitTemplate);
            }
        }

        @RequiredArgsConstructor
        private static class Sender {

            private final RabbitTemplate rabbitTemplate;

            private AtomicInteger messageCounter = new AtomicInteger(0);

            private Logger log = LoggerFactory.getLogger(getClass());

            @Scheduled(fixedDelay = 1000, initialDelay = 1000)
            void send() {

                int messageNumber = messageCounter.getAndIncrement();
                String routingKeyString = RoutingKey.values()[messageNumber%3].toString();
                String message = messageNumber + "-" + routingKeyString;
                rabbitTemplate.convertAndSend(EXCHANGE_NAME,
                        routingKeyString,
                        message);
                log.info("Send '{}' with routing key '{}'",
                        message,
                        routingKeyString);
            }
        }

        @Test
        void test() throws InterruptedException {
            TimeUnit.SECONDS.sleep(60);
        }
    }

    @SpringBootTest
    @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
    static class ReceiverRedGreenTest {

        @TestConfiguration
        static class RabbitConfig {

            @Bean
            DirectExchange directExchange() {
                return new DirectExchange(EXCHANGE_NAME, true, true);
            }

            @Bean
            Queue queue() {
                return new AnonymousQueue();
            }

            @Bean
            Binding bindingRed(DirectExchange directExchange, Queue queue) {
                return BindingBuilder.bind(queue)
                        .to(directExchange)
                        .with(RoutingKey.RED);
            }

            @Bean
            Binding bindingGreen(DirectExchange directExchange, Queue queue) {
                return BindingBuilder.bind(queue)
                        .to(directExchange)
                        .with(RoutingKey.GREEN);
            }

            @Bean
            Receiver receiver() {
                return new Receiver();
            }
        }

        private static class Receiver {

            static final String RECEIVER_ID = "receiver-red-green";

            private Logger log = LoggerFactory.getLogger(getClass());

            @RabbitListener(id = RECEIVER_ID, queues = "#{queue.name}")
            void receive(String message) {
                log.info("Receive '{}'", message);
            }
        }

        @Test
        void test() throws InterruptedException {
            TimeUnit.SECONDS.sleep(60);
        }

        @TestConfiguration
        @RabbitListenerTest
        static class RabbitListenerTestConfig {}

        @Autowired
        private RabbitTemplate rabbitTemplate;

        @Autowired
        private RabbitListenerTestHarness rabbitListenerTestHarness;

        @Test
        void givenReceiverBindQueueToExchangeWithRoutingKey_whenSendMessageWithVariousRoutingKey_thenReceiverReceiveOnlyMessageWithSelectedRoutingKey() {

            Receiver receiver = rabbitListenerTestHarness.getSpy(Receiver.RECEIVER_ID);

            // when
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, RoutingKey.RED.toString(), "0-RED");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, RoutingKey.GREEN.toString(), "1-GREEN");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, RoutingKey.BLUE.toString(), "2-BLUE");

            // then
            BDDMockito.then(receiver).should().receive("0-RED");
            BDDMockito.then(receiver).should().receive("1-GREEN");
            BDDMockito.then(receiver).shouldHaveNoMoreInteractions();
        }
    }
}
