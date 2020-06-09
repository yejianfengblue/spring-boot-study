package com.yejianfengblue.spring.boot.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import javax.validation.Valid;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled("Not finished. Continue after study micrometer")
@SpringBootTest
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
class KafkaListener_Metrics_Test {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    TestKafkaConfig testKafkaConfig;

    @Autowired
    MeterRegistry meterRegistry;

    private static final CountDownLatch legalLatch = new CountDownLatch(1);

    private static final CountDownLatch illegalLatch = new CountDownLatch(1);

    private static final String TOPIC = "payload-validation-test-topic";

    @Test
    @SneakyThrows
    void whenSendToTopic_thenKafkaListenerIsTriggered() {

        kafkaTemplate.send(TOPIC, objectMapper.writeValueAsString(new User("legalUsername")));
        assertThat(this.legalLatch.await(10, TimeUnit.SECONDS)).isTrue();

        kafkaTemplate.send(TOPIC, objectMapper.writeValueAsString(new User("x")));
        assertThat(this.illegalLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(testKafkaConfig.validationException).isInstanceOf(MethodArgumentNotValidException.class);
        MethodArgumentNotValidException notValidException = (MethodArgumentNotValidException) testKafkaConfig.validationException;
        log.info("failed message : {}", notValidException.getFailedMessage());

        this.meterRegistry.get("?");  // TODO after study micrometer and what meter name is available

    }

    @TestConfiguration
    @RequiredArgsConstructor
    @EnableKafka
    static class TestKafkaConfig implements KafkaListenerConfigurer {

        @Autowired
        private LocalValidatorFactoryBean validator;

        @Autowired
        private MeterRegistry meterRegistry;

        @Override
        public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
            registrar.setValidator(this.validator);
        }

        private Throwable validationException;

        @KafkaListener(id = "validation", topics = TOPIC,
                errorHandler = "validationErrorHandler")
        void receive(@Payload @Valid User user) {

            legalLatch.countDown();

            log.info("received user : {}", user);
        }

        @Bean
        public KafkaListenerErrorHandler validationErrorHandler(TestKafkaConfig config) {
            return (message, exception) -> {

                illegalLatch.countDown();
                config.validationException = exception.getCause();

                log.error("validationErrorHandler.message : {}", message.getPayload());
                log.error("validationErrorHandler.exception : {}", exception.getCause().getMessage());

                return null;
            };
        }

        @Bean
        public JsonMessageConverter jsonMessageConverter() {
            return new JsonMessageConverter();
        }

        @Bean
        public Map<String, Object> producerConfigs() {
            return KafkaTestUtils.producerProps("PLAINTEXT://localhost:9092");
        }

        @Bean
        public ProducerFactory<String, String> producerFactory() {

            DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerConfigs());
            producerFactory.addListener(new MicrometerProducerListener<String, String>(
                    meterRegistry,
                    Collections.singletonList(new ImmutableTag("producerTag", "producerTagValue"))));

            return producerFactory;
        }

        @Bean
        public KafkaTemplate<String, String> kafkaJsonTemplate() {

            KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
            kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());

            return kafkaTemplate;
        }
    }

    @Value
    static class User {

        @Size(min = 10)
        private String username;

        @JsonCreator
        public User(@JsonProperty("username") String username) {
            this.username = username;
        }
    }

}
