package com.yejianfengblue.spring.boot.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MongoDB transaction (built on top of session) is supported on MongoDB replica set.
 * I test with a mongodb docker container:
 * <ol>
 *     <li>Run a docker container, map container port 27017 to localhost:27001, because my localhost:27017 is used by a standalone mongodb: <br/>
 *         {@code $ docker run --detach -p 27001:27017 --name mongodb-cluster mongo mongod --replSet mongodb-replica-set}</li>
*      <li>Run mongo shell inside the container: <br/>
 *         {@code $ docker exec -it mongo-cluster mongo}</li>
 *     <li>{@code > db.isMaster()} <br/>
 *         should say it is neither master nor secondary</li>
 *     <li>{@code > rs.initiate()} <br/>
 *         to initiate the new replica set with a default configuration</li>
 *     <li>Now the shell indicator should change from ">" to "mongodb-replica-set:PRIMARY>"</li>
 *     <li>{@code > db.isMaster()} <br/>
 *         should say it is master</li>
 * </ol>
 * @see <a href = "https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/" >
 *     Convert standalone to replica set</a>
 */
// enable mongodb driver debug log to see mongodb driver command saying commit transaction or abort transaction
@SpringBootTest(properties = {"logging.level.org.mongodb.driver.protocol = debug"})
@EnableMongoRepositories(considerNestedRepositories = true, basePackageClasses = MongoTransactionTest.class)
@EnableTransactionManagement
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
public class MongoTransactionTest {

    private static final String DB_NAME = "spring-data-mongo-tx-test";

    private static final String COLLECTION_NAME = "MongoTransactionTestProcess";

    @Document(COLLECTION_NAME)
    @Data
    @AllArgsConstructor
    private static class Process {

        @Id
        private Integer id;

        private State state;

        int transitionCount;

        void incrementTransitionCount() {
            transitionCount++;
        }

        enum State {

            CREATED, ACTIVE, DONE
        }
    }

    @Repository("MongoTransactionTestProcessRepository")
    interface ProcessRepository extends MongoRepository<Process, Integer> {}

    @TestConfiguration
    static class MongoConfig extends AbstractMongoClientConfiguration {

        @Bean
        MongoTransactionManager transactionManager(MongoDbFactory mongoDbFactory) {
            log.info("MongoTransactionManager bean");
            return new MongoTransactionManager(mongoDbFactory);
        }

        @Bean
        @Override
        public MongoClient mongoClient() {
            return MongoClients.create("mongodb://localhost:27001");
        }

        @Override
        protected String getDatabaseName() {
            return DB_NAME;
        }
    }

    @TestConfiguration
    @Import(TransitionService.class)
    static class TestConfig {}

    @Service
    @RequiredArgsConstructor
    static class TransitionService {

        private final ProcessRepository processRepository;

        private final MongoTemplate mongoTemplate;

        /**
         * <ul>
         * <li>For process with ID (id % 3 != 0), state will be updated to DONE.
         * <li>For process with ID (id % 3 == 0), that is, 3, 6, 9, state update will be rollback to CREATED.
         * </ul>
         *
         */
        @Transactional
        public void run(Integer processId) {

            Process process = processRepository.findById(processId).get();

            process.setState(Process.State.ACTIVE);
            process.incrementTransitionCount();
            process = processRepository.save(process);
            log.info("Change process {} state to {}", processId, process.getState());

            // throw exception to abort the transaction and rollback
            Assert.state(process.getId() % 3 != 0, "We're sorry but we needed to drop it");

            process.setState(Process.State.DONE);
            process.incrementTransitionCount();
            process = processRepository.save(process);
            log.info("Change process {} state to {}", processId, process.getState());
        }
    }

    @BeforeEach
    @AfterEach
    void dropCollection() {
        mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME).drop();
    }

    @DisplayName("process id 3/6/9 rollback while other commit")
    @Test
    void txCommitRollback(@Autowired TransitionService transientService,
                          @Autowired ProcessRepository processRepository) {

        final AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < 10; i++) {

            Process process = new Process(counter.incrementAndGet(), Process.State.CREATED, 0);
            process = processRepository.save(process);

            try {
                transientService.run(process.getId());
                Assertions.assertThat(stateInDb(process.getId())).isEqualTo(Process.State.DONE);
            } catch (IllegalStateException e) {
                // The Assert.state() in TransientService.run() throws IllegalStateException if fails
                Assertions.assertThat(stateInDb(process.getId())).isEqualTo(Process.State.CREATED);
            }
        }

        log.info("Let's verify the final result in database");
        FindIterable<org.bson.Document> results = mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME)
                .find();
        assertTrue(
                StreamSupport.stream(results.spliterator(), false).allMatch(
                        d -> {
                            if (d.getInteger("_id") % 3 != 0) {
                                return d.getString("state").equals(Process.State.DONE.toString());
                            } else {
                                return d.getString("state").equals(Process.State.CREATED.toString());
                            }
                        }
                )
        );

        // see org.mongodb.driver.protocol.command debug log: Sending command '{"commitTransaction"...
        // and Sending command '{"abortTransaction"...
    }

    @Autowired
    private MongoClient mongoClient;

    Process.State stateInDb(Integer processId) {

        return Process.State.valueOf(
                mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME)
                        .find(Filters.eq("_id", processId))
                        .projection(Projections.include("state"))
                        .first()
                        .get("state", String.class)
        );
    }
}

