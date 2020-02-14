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
 * Monitor the message queue from RabbitMQ management UI, should see no redelivered.
 * The rejected message will be routed to the dead letter queue.
 *
 * @author yejianfengblue
 */
@SpringBootTest
@EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
class DlxWithoutDeadLetterRoutingKeyTest {

    private static final String EXCHANGE_NAME = "spring-boot-study-rabbitmq-dlx-without-dead-letter-routing-key-test-exchange";

    private static final String QUEUE_NAME = "spring-boot-study-rabbitmq-dlx-without-dead-letter-routing-key-test-queue";

    private final Logger log = LoggerFactory.getLogger(getClass());

    @TestConfiguration
    static class RabbitmqConfig {

        @Bean
        DirectExchange messageExchange() {
            return ExchangeBuilder.directExchange(EXCHANGE_NAME)
                    .autoDelete()
                    .build();
        }

        /**
         * Declare a message queue, with dlx and dlq, non-auto-deleted, expires after 5min,
         * so we can monitor this queue from management web UI and this queue will be auto deleted after expires.
         */
        @Bean
        Queue messageQueue() {
            return QueueBuilder.durable(QUEUE_NAME)
                    .deadLetterExchange("")  // use default exchange as dead letter exchange
                    // not change the initial routing key of the message
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
        private void receive(String message) throws InterruptedException {

            log.error("{} - Receive '{}', but reject and don't requeue", counter.incrementAndGet(), message);
            TimeUnit.SECONDS.sleep(5);
            throw new AmqpRejectAndDontRequeueException("Throw exception to simulate reject and don't requeue");
        }

    }

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Test
    void givenDeadLetterExchangeWithoutDeadLetterRoutingKey_whenRejectMessageAndDontRequeue_thenMessageIsRoutedTo() throws InterruptedException {

        byte[] message = ("hello at " + LocalDateTime.now().toString()).getBytes();
        rabbitTemplate.convertAndSend(EXCHANGE_NAME, QUEUE_NAME, message);
        TimeUnit.SECONDS.sleep(33);
    }

}
