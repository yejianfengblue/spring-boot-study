package com.yejianfengblue.spring.boot.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
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
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import javax.validation.Valid;
import javax.validation.constraints.Size;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
class KafkaListener_PayloadValidation_Test {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    TestKafkaConfig testKafkaConfig;

    private static final CountDownLatch legalLatch = new CountDownLatch(1);

    private static final CountDownLatch illegalLatch = new CountDownLatch(1);

    private static final String TOPIC = "payload-validation-test-topic";

    @BeforeEach
    void configObjectMapper() {
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Test
    @SneakyThrows
    void whenSendToTopic_thenKafkaListenerIsTriggered() {

        kafkaTemplate.send(TOPIC, objectMapper.writeValueAsString(new User("legalUsername")));
        assertThat(this.legalLatch.await(10, TimeUnit.SECONDS)).isTrue();

        kafkaTemplate.send(TOPIC, objectMapper.writeValueAsString(new User("x")));
        assertThat(this.illegalLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(testKafkaConfig.validationException).isInstanceOf(MethodArgumentNotValidException.class);
        MethodArgumentNotValidException notValidException = (MethodArgumentNotValidException)testKafkaConfig.validationException;
        log.info("failed message : {}", notValidException.getFailedMessage());

    }

    @TestConfiguration
    @RequiredArgsConstructor
    @EnableKafka
    static class TestKafkaConfig implements KafkaListenerConfigurer {

        @Autowired
        private LocalValidatorFactoryBean validator;

        @Override
        public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
            registrar.setValidator(this.validator);
        }

        private Throwable validationException;

        @KafkaListener(id = "validation", topics = TOPIC,
                errorHandler = "validationErrorHandler",
                containerFactory = "kafkaJsonListenerContainerFactory")
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
        public Map<String, Object> consumerConfigs() {
            Map<String, Object> consumerProps =
                    KafkaTestUtils.consumerProps("PLAINTEXT://localhost:9092",
                            "payload-validation-group",
                            "false");
            return consumerProps;
        }

        @Bean
        public DefaultKafkaConsumerFactory<Integer, String> consumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerConfigs());
        }

        @Bean
        public KafkaListenerContainerFactory<?> kafkaJsonListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            JsonMessageConverter converter = new JsonMessageConverter();
            DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
            typeMapper.addTrustedPackages("*");
            converter.setTypeMapper(typeMapper);
            factory.setMessageConverter(converter);
            return factory;
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
