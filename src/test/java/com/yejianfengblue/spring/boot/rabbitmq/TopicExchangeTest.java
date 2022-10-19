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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * A Rabbit MQ example where sender send message to topic exchange with various routingKey, and receiver is able to
 * receive message selectively by routingKey pattern (topic).
 * <ul>
 * <li>Run a RabbitMQ server on localhost:5672 with default user guest:guest before start the two tests.
 * <li>Run method test() in the receiver first and then sender, because receiver creates exchange if not exists.
 * <li>If an exchange named {@link #EXCHANGE_NAME} already exists but it's not "durable and auto-delete",
 * delete it first before run subscriber, because the exchange is declared to be durable and auto-delete in the
 * two Subscribers and exchanges with same name but diff features (durable and auto-delete) are mutually exclusive.
 * </ul>
 * @author yejianfengblue
 */
class TopicExchangeTest {

    private static final String EXCHANGE_NAME = "spring-boot-study-rabbitmq-topic-exchange-test-exchange";

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

            private final String[] monsterCardRoutingKeys = {
                    "normal.dark.spellcaster",
                    "normal.dark.dragon",
                    "normal.light.spellcaster",
                    "normal.light.dragon",

                    "effect.dark.spellcaster",
                    "effect.dark.dragon",
                    "effect.light.spellcaster",
                    "effect.light.dragon",
            };

            private Logger log = LoggerFactory.getLogger(getClass());

            @Scheduled(fixedDelay = 1000, initialDelay = 1000)
            void send() {

                int messageNumber = messageCounter.getAndIncrement();
                String routingKeyString = monsterCardRoutingKeys[messageNumber % 8];
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
    static class ReceiverTest {

        @TestConfiguration
        static class RabbitConfig {

            @Bean
            TopicExchange topicExchange() {
                return new TopicExchange(EXCHANGE_NAME, true, true);
            }

            @Bean
            Queue queue() {
                return new AnonymousQueue();
            }

            @Bean
            Binding bindingMonsterDark(TopicExchange topicExchange, Queue queue) {
                return BindingBuilder.bind(queue)
                        .to(topicExchange)
                        .with("*.dark.*");
            }

            @Bean
            Binding bindingLightDragon(TopicExchange topicExchange, Queue queue) {
                return BindingBuilder.bind(queue)
                        .to(topicExchange)
                        .with("*.light.dragon");
            }

            @Bean
            Receiver receiver() {
                return new Receiver();
            }
        }

        private static class Receiver {

            static final String RECEIVER_ID = "receiver-all-dark-monster-and-all-light-dragon";

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
        void givenReceiverBindQueueToTopicExchangeWithRoutingKeyWithWildcards_whenSendMessageWithVariousRoutingKey_thenReceiverReceiveOnlyMessageWithRoutingKeyMatchingPattern() {

            Receiver receiver = rabbitListenerTestHarness.getSpy(Receiver.RECEIVER_ID);

            // when
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "normal.dark.spellcaster", "0-normal.dark.spellcaster");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "normal.dark.dragon", "1-normal.dark.dragon");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "normal.light.spellcaster", "2-normal.light.spellcaster");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "normal.light.dragon", "3-normal.light.dragon");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "effect.dark.spellcaster", "4-effect.dark.spellcaster");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "effect.dark.dragon", "5-effect.dark.dragon");
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, "effect.light.spellcaster", "6-effect.light.spellcaster");
            rabbitTemplate.convertSendAndReceive(EXCHANGE_NAME, "effect.light.dragon", "7-effect.light.dragon");
            // the last convertSendAndReceive() ensure all messages are sent before verification

            // then
            verify(receiver, times(6)).receive(anyString());
            BDDMockito.then(receiver).should().receive("0-normal.dark.spellcaster");
            BDDMockito.then(receiver).should().receive("1-normal.dark.dragon");
            BDDMockito.then(receiver).should().receive("3-normal.light.dragon");
            BDDMockito.then(receiver).should().receive("4-effect.dark.spellcaster");
            BDDMockito.then(receiver).should().receive("5-effect.dark.dragon");
            BDDMockito.then(receiver).should().receive("7-effect.light.dragon");
            BDDMockito.then(receiver).shouldHaveNoMoreInteractions();
        }
    }
}
