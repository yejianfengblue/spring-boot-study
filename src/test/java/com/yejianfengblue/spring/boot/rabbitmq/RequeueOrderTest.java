package com.yejianfengblue.spring.boot.rabbitmq;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Monitor the queue from RabbitMQ management UI, should see many redelivered.
 * From the app log, after 1st message is rejected, 2nd message still arrives, coz 1st negative ack takes time arriving
 * broker.
 * If 1st is bad message and 2nd is good, 2nd (latter) message may be processed before 1st.
 * If 1st is somehow lost in the network, 2nd comes, and then 1st redelivered later.
 * It also proves that in async scenario, message FIFO is trivial.
 * Get message from management UI, the order is same as originally sent.
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "logging.level.org.springframework.amqp.rabbit.listener.ConditionalRejectingErrorHandler = ERROR"
})
@EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
class RequeueOrderTest {

    private static final String EXCHANGE_NAME = "spring-boot-study-rabbitmq-requeue-order-test-exchange";

    private static final String QUEUE_NAME = "spring-boot-study-rabbitmq-requeue-order-test-queue";

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
        private void receive(String message) throws InterruptedException {

            log.error("{} - Receive '{}', but throw exception to simulate requeue", counter.incrementAndGet(), message);
            TimeUnit.MILLISECONDS.sleep(1);
            throw new ImmediateRequeueAmqpException("Throw exception to simulate requeue");
        }

    }

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Test
    void givenReceiverAlwaysThrowImmediateRequeueAmqpException_whenSendMultipleMessages_thenMessageIsRequeueInHeadAndRedeliveredInOrder() throws InterruptedException {

        for (int i = 0; i < 1000; i++) {
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, QUEUE_NAME, "Say hello " + i);
        }
        TimeUnit.SECONDS.sleep(1);
    }
}
