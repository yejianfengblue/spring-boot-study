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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Monitor the message queue from RabbitMQ management UI, should see no redelivered.
 * The rejected message will be routed to the dead letter queue.
 *
 * @author yejianfengblue
 */
@SpringBootTest
@EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
class DlxTest {

    private static final String EXCHANGE_NAME = "spring-boot-study-rabbitmq-dlx-test-exchange";

    private static final String QUEUE_NAME = "spring-boot-study-rabbitmq-dlx-test-queue";

    private static final String DLQ_NAME = QUEUE_NAME + ".dlq";

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
         * Declare a message queue, with dlx and dead letter routing key, non-auto-deleted, expires after 5min,
         * so we can monitor this queue from management web UI and this queue will be auto deleted after expires.
         */
        @Bean
        Queue messageQueue() {
            return QueueBuilder.durable(QUEUE_NAME)
                    .deadLetterExchange("")  // use default exchange as dead letter exchange
                    .deadLetterRoutingKey(DLQ_NAME)  // change the initial routing key of the message
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
        Queue deadLetterQueue() {
            return QueueBuilder.durable(DLQ_NAME)
                    .expires(300_000)
                    .build();
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
    void givenDeadLetterExchangeAndDeadLetterRoutingKey_whenRejectMessageAndDontRequeue_thenMessageIsRoutedToDlx() throws InterruptedException {

        byte[] message = ("hello at " + LocalDateTime.now().toString()).getBytes();
        rabbitTemplate.convertAndSend(EXCHANGE_NAME, QUEUE_NAME, message);
        TimeUnit.SECONDS.sleep(1);

        Message dlqMessage = rabbitTemplate.receive(DLQ_NAME);
        assertNotNull(dlqMessage);
        assertArrayEquals(message, dlqMessage.getBody());
        MessageProperties dlqMessageMessageProperties = dlqMessage.getMessageProperties();
        List<Map<String, ?>> xDeathHeaderList = dlqMessageMessageProperties.getXDeathHeader();
        assertEquals(1, xDeathHeaderList.size());
        Map<String, ?> xDeathHeader = xDeathHeaderList.get(0);
        log.info(xDeathHeader.toString());
        assertEquals("rejected", xDeathHeader.get("reason"));
        assertEquals(1L, xDeathHeader.get("count"));
        assertEquals(EXCHANGE_NAME, xDeathHeader.get("exchange"));
        assertEquals(QUEUE_NAME, xDeathHeader.get("queue"));
        List<String> routingKeys = (List<String>) xDeathHeader.get("routing-keys");
        assertEquals(1, routingKeys.size());
        assertEquals(QUEUE_NAME, routingKeys.get(0));
    }

}
