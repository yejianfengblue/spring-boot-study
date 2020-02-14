package com.yejianfengblue.spring.boot.rabbitmq;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

/**
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "logging.level.org.springframework.amqp = debug"
})
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
class PublishReturnWithoutConfirmTest {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    private Logger log = LoggerFactory.getLogger(getClass());

    @Test
    void givenRabbitTemplateSetMandatoryAndReturnCallbackAndNoConfirm_whenSendMessageAndMessageIsNotRoutedToQueue_thenReturnMessageIsReturnedAndReturnCallbackIsInvoked() {

        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setReturnCallback(
                (message, replyCode, replyText, exchange, routingKey) ->
                        log.info("message = {}, replyCode = {}, replyText = {}, exchange = {}, routingKey = {}",
                                message, replyCode, replyText, exchange, routingKey));

        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend("", "non-existent-routing-key", "hello", correlationData);
    }
}



