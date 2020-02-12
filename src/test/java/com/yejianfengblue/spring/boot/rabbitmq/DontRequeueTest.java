package com.yejianfengblue.spring.boot.rabbitmq;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Monitor the queue from RabbitMQ management UI, should see no redelivered.
 * Monitor this app log, should see the receiver receives only once.
 *
 * @author yejianfengblue
 */
@SpringBootTest
@EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
class DontRequeueTest {

    private static final String EXCHANGE_NAME = "spring-boot-study-rabbitmq-dont-requeue-test-exchange";

    private static final String QUEUE_NAME = "spring-boot-study-rabbitmq-dont-requeue-test-queue";

    @TestConfiguration
    static class RabbitmqConfig {

        @Bean
        DirectExchange messageExchange() {
            return ExchangeBuilder.directExchange(EXCHANGE_NAME)
                    .autoDelete()
                    .build();
        }

        /**
         * Declare a message queue, non-auto-deleted, expires after 5min,
         * so we can monitor this queue from management web UI and this queue will be auto deleted after expires
         */
        @Bean
        Queue messageQueue() {
            return QueueBuilder.durable(QUEUE_NAME)
                    .expires(300_000)  // queue expires after 5min
                    .build();
        }

        @Bean
        Binding messageBinding() {
            return BindingBuilder.bind(messageQueue())
                    .to(messageExchange())
                    .withQueueName();
        }

        @Bean
        Receiver receiver() {
            return new Receiver();
        }

    }

    private static class Receiver {

        private Logger log = LoggerFactory.getLogger(getClass());

        private AtomicInteger counter = new AtomicInteger(0);

        @RabbitListener(queues = QUEUE_NAME)
        private void receive(String message) {

            log.error("{} - Receive '{}', but reject and don't requeue", counter.incrementAndGet(), message);
            throw new AmqpRejectAndDontRequeueException("Throw exception to simulate reject and don't requeue");
        }

    }

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Test
    void givenReceiverAlwaysAmqpRejectAndDontRequeueException_whenSendMessage_thenMessageIsNotRequeueAndDiscarded() throws InterruptedException {

        rabbitTemplate.convertAndSend(EXCHANGE_NAME, QUEUE_NAME, "say hello at " + LocalDateTime.now().toString());
        TimeUnit.SECONDS.sleep(1);
    }
}
