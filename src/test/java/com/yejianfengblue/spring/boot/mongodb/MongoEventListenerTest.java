package com.yejianfengblue.spring.boot.mongodb;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.event.LoggingEventListener;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

/**
 * Before run this test, start a clean MongoDB on localhost:27017
 */
@SpringBootTest
@EnableMongoRepositories(considerNestedRepositories = true, basePackageClasses = MongoEventListenerTest.class)
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
public class MongoEventListenerTest {

    @Document("MongoEventListenerTestUser")
    @Data
    private static class User {

        @Id
        private final String username;
    }

    @TestConfiguration  // class access modifier must not be private
    static class Config {

        @Bean
        LoggingEventListener mongoEventListener() {
            return new LoggingEventListener();
        }
    }

    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    void givenLoggingEventListener_whenCrud_thenEventLoggingPrinted() {

        log.info("When save ------------------------------");
        User yugi = new User("yugi");
        yugi = mongoTemplate.insert(yugi);

        log.info("When find ------------------------------");
        yugi = mongoTemplate.findById(yugi.getUsername(), User.class);

        log.info("When delete ----------------------------");
        mongoTemplate.remove(yugi);
    }
}
