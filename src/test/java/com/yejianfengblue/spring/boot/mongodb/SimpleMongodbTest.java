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
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Before run this test, start a clean MongoDB on localhost:27017
 */
@SpringBootTest
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
class SimpleMongodbTest {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Data
    @Document("simpleMongodbTestUser")
    private static class User {

        @Id
        private String id;

        private String name;

        private LocalDateTime birthDateTime;

        private Instant createdTime;

        User(String name, LocalDateTime birthDateTime, Instant createdTime) {
            this.name = name;
            this.birthDateTime = birthDateTime;
            this.createdTime = createdTime;
        }
    }

    @Test
    void test() {

        mongoTemplate.dropCollection(User.class);

        User kaibaSeto = mongoTemplate.save(new User("Kaiba Seto", LocalDateTime.of(2000, 1, 1, 0, 0, 0), Instant.now()));
        assertThat(kaibaSeto.getId()).isNotNull();
        log.info(mongoTemplate.findAll(User.class).toString());
    }

}
