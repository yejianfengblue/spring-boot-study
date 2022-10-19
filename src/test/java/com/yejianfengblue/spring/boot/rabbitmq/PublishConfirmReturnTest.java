package com.yejianfengblue.spring.boot.rabbitmq;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "spring.rabbitmq.publisher-confirm-type = correlated",
        "logging.level.org.springframework.amqp = debug"
})
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
class PublishConfirmReturnTest {

    private static final String QUEUE_NAME = "spring-boot-study-rabbitmq-publish-confirm-return-test-queue";

    @Autowired
    private RabbitTemplate rabbitTemplate;

    private Logger log = LoggerFactory.getLogger(getClass());

    @TestConfiguration
    static class RabbitmqConfig {

        /**
         * Declare a message queue, with dlx and dead letter routing key, non-auto-deleted, expires after 5min,
         * so we can monitor this queue from management web UI and this queue will be auto deleted after expires.
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
                    .to(new DirectExchange(""))
                    .withQueueName();
        }

    }

    @Test
    void givenRabbitmqPublisherConfirmTypeCorrelatedAndRabbitTemplateSetConfirmCallback_whenSendMessageSuccessfully_thenPublishConfirmIsReturnedWithAckTrueAndConfirmCallbackIsInvoked() throws InterruptedException, TimeoutException, ExecutionException {

        rabbitTemplate.setConfirmCallback(
                (correlationData, ack, cause) ->
                        log.info("correlationData = {}, ack = {}, cause = {}",
                                correlationData,
                                ack,
                                cause));

        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend("", QUEUE_NAME, "hello", correlationData);

        CorrelationData.Confirm confirm = correlationData.getFuture().get(10, TimeUnit.SECONDS);
        assertTrue(confirm.isAck());
        assertNull(confirm.getReason());
        assertNull(correlationData.getReturnedMessage());
    }

    @Test
    void givenRabbitmqPublisherConfirmTypeCorrelatedAndRabbitTemplateSetConfirmCallback_whenSendMessageToNonExistentExchange_thenPublishConfirmIsReturnedWithAckFalseAndReasonContainErrorMessage() throws InterruptedException, TimeoutException, ExecutionException {

        rabbitTemplate.setConfirmCallback(
                (correlationData, ack, cause) ->
                        log.info("correlationData = {}, ack = {}, cause = {}",
                                correlationData,
                                ack,
                                cause)
        );

        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend("non-existent-exchange", "non-existent-routing-key", "hello", correlationData);

        CorrelationData.Confirm confirm = correlationData.getFuture().get(10, TimeUnit.SECONDS);
        assertFalse(confirm.isAck());
        assertEquals("channel error; protocol method: #method<channel.close>(reply-code=404, reply-text=NOT_FOUND - no exchange 'non-existent-exchange' in vhost '/', class-id=60, method-id=40)", confirm.getReason());
        assertNull(correlationData.getReturnedMessage());
    }

    @Test
    void givenRabbitmqPublisherConfirmTypeCorrelatedAndRabbitTemplateSetMandatoryAndReturnCallback_whenSendMessageAndMessageIsNotRoutedToQueue_thenReturnMessageIsAvailableInCorrelationDataAndReturnCallbackIsInvoked() throws InterruptedException, TimeoutException, ExecutionException {

        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setReturnCallback(
                (message, replyCode, replyText, exchange, routingKey) ->
                    log.info("message = {}, replyCode = {}, replyText = {}, exchange = {}, routingKey = {}",
                            message, replyCode, replyText, exchange, routingKey)
                );
        rabbitTemplate.setConfirmCallback(
                (correlationData, ack, cause) ->
                        log.info("correlationData = {}, ack = {}, cause = {}",
                                correlationData,
                                ack,
                                cause)
                );

        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend("", "non-existent-routing-key", "hello", correlationData);
        CorrelationData.Confirm confirm = correlationData.getFuture().get(10, TimeUnit.SECONDS);
        assertTrue(confirm.isAck());

        log.info("correlationData.getReturnedMessage() = {}", correlationData.getReturnedMessage());
        Message returnedMessage = correlationData.getReturnedMessage();
        assertNotNull(returnedMessage);
        assertArrayEquals("hello".getBytes(), returnedMessage.getBody());
        MessageProperties messageProperties = returnedMessage.getMessageProperties();
        assertEquals("text/plain", messageProperties.getContentType());
        assertEquals("UTF-8", messageProperties.getContentEncoding());
        assertEquals(MessageDeliveryMode.PERSISTENT, messageProperties.getReceivedDeliveryMode());
        assertFalse(messageProperties.getRedelivered());
        assertEquals("", messageProperties.getReceivedExchange());
        assertEquals("non-existent-routing-key", messageProperties.getReceivedRoutingKey());
    }

}



