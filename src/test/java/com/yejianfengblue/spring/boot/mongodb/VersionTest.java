package com.yejianfengblue.spring.boot.mongodb;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
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
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.event.LoggingEventListener;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Before run this test, start a clean MongoDB on localhost:27017
 */
@SpringBootTest
@EnableMongoRepositories(considerNestedRepositories = true, basePackageClasses = VersionTest.class)
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
public class VersionTest {

    @Document("VersionTestUser")
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    @Getter
    @ToString
    private static class User {

        @Id
        private String id;

        @EqualsAndHashCode.Include
        private String username;

        @Setter
        private int money;

        @Version
        private Integer version;

        public User(String username) {
            this.username = username;
            this.money = 0;
        }
    }

    interface UserRepos extends MongoRepository<User, String> {

        Optional<User> findByUsername(String userfname);
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

    @Autowired
    private UserRepos userRepos;

    @BeforeEach
    void setup() {
        mongoTemplate.dropCollection("VersionTestUser");
    }

    @AfterEach
    void clean() {
        mongoTemplate.dropCollection("VersionTestUser");
    }

    @Test
    void givenVersion_whenUpdate_thenVersionGetIncremented() {

        log.info("When save ------------------------------");
        User yugi = new User("yugi");
        assertThat(yugi.getVersion()).isNull();
        yugi = userRepos.save(yugi);

        log.info("When find ------------------------------");
        yugi = userRepos.findByUsername(yugi.username).get();
        assertThat(yugi.getVersion()).isEqualTo(0);

        log.info("When update ------------------------------");
        yugi.setMoney(100);
        yugi = userRepos.save(yugi);
        assertThat(yugi.getVersion()).isEqualTo(1);
    }

}
