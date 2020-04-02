package com.yejianfengblue.spring.boot.mongodb;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.geo.Box;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Polygon;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.repository.init.AbstractRepositoryPopulatorFactoryBean;
import org.springframework.data.repository.init.Jackson2RepositoryPopulatorFactoryBean;
import org.springframework.stereotype.Repository;

import java.util.List;


/**
 * Before run this test, start a clean MongoDB on localhost:27017
 */
@SpringBootTest
@EnableMongoRepositories(considerNestedRepositories = true, basePackageClasses = GeoTest.class)
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
public class GeoTest {

    @Document("geoTestStarbucks")
    @Data
    private static class Starbucks {

        private String id;

        private String name;

        private String street;

        private String city;

        /**
         * {@code location} is stored in GeoJSON format.
         *
         * <pre>
         * <code>
         * {
         *   "type" : "Point",
         *   "coordinates" : [ x, y ]
         * }
         * </code>
         * </pre>
         */
        private GeoJsonPoint location;
    }

    @Repository("GeoTestStarbucksRepository")
    interface StarbucksRepository extends MongoRepository<Starbucks, String> {

        List<Starbucks> findByLocationWithin(Polygon polygon);
    }

    @TestConfiguration
    static class Config {

        /**
         * Read JSON data from classpath file and insert
         */
        @Bean
        AbstractRepositoryPopulatorFactoryBean repositoryPopulator() {

            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.addMixIn(GeoJsonPoint.class, GeoJsonPointMixin.class);
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            Jackson2RepositoryPopulatorFactoryBean jackson2RepositoryPopulatorFactoryBean = new Jackson2RepositoryPopulatorFactoryBean();
            jackson2RepositoryPopulatorFactoryBean.setResources(
                    new Resource[]{new ClassPathResource("geo-starbucks-in-nyc.json")});
            jackson2RepositoryPopulatorFactoryBean.setMapper(objectMapper);

            return jackson2RepositoryPopulatorFactoryBean;
        }

        static abstract class GeoJsonPointMixin {
            GeoJsonPointMixin(@JsonProperty("longitude") double x, @JsonProperty("latitude") double y) {}
        }
    }

    @Autowired
    private StarbucksRepository starbucksRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Test
    void test_within() {

        GeoJsonPolygon GEO_JSON_POLYGON = new GeoJsonPolygon(
                new Point(-73.992514, 40.758934),
                new Point(-73.961138, 40.760348),
                new Point(-73.991658, 40.730006),
                new Point(-73.992514, 40.758934)
        );
        List<Starbucks> starbucksList = starbucksRepository.findByLocationWithin(GEO_JSON_POLYGON);

        log.info("size = {}", starbucksList.size());
        starbucksList.forEach(starbucks ->
                log.info("id = {}, (x, y) = ({}, {})",
                        starbucks.getId(),
                        starbucks.getLocation().getX(),
                        starbucks.getLocation().getY()));
    }

    @Test
    void test_intersects() {

        GeoJsonPolygon GEO_JSON_POLYGON = new GeoJsonPolygon(
                new Point(-73.992514, 40.758934),
                new Point(-73.961138, 40.760348),
                new Point(-73.991658, 40.730006),
                new Point(-73.992514, 40.758934)
        );

        List<Starbucks> starbucksList = mongoTemplate.aggregate(Aggregation.newAggregation(Starbucks.class,
                Aggregation.match(
                        Criteria.where("location")
                                .intersects(GEO_JSON_POLYGON))),
                Starbucks.class).getMappedResults();

        log.info("size = {}", starbucksList.size());
        starbucksList.forEach(starbucks ->
                log.info("id = {}, (x, y) = ({}, {})",
                        starbucks.getId(),
                        starbucks.getLocation().getX(),
                        starbucks.getLocation().getY()));
    }

    @Test
    void test_within_circle() {

        Circle circle = new Circle(new Point(-73.992514, 40.758934), 0.01);

        List<Starbucks> starbucksList = mongoTemplate.find(
                new Query(
                        Criteria.where("location").within(circle)),
                Starbucks.class);

        log.info("size = {}", starbucksList.size());
        starbucksList.forEach(starbucks ->
                log.info("id = {}, (x, y) = ({}, {})",
                        starbucks.getId(),
                        starbucks.getLocation().getX(),
                        starbucks.getLocation().getY()));
    }

    @Test
    void test_withinSphere_circle() {

        Circle circle = new Circle(new Point(-73.992514, 40.758934), 0.0001);

        List<Starbucks> starbucksList = mongoTemplate.find(
                new Query(
                        Criteria.where("location")
                                .withinSphere(circle)),
                Starbucks.class);

        log.info("size = {}", starbucksList.size());
        starbucksList.forEach(starbucks ->
                log.info("id = {}, (x, y) = ({}, {})",
                        starbucks.getId(),
                        starbucks.getLocation().getX(),
                        starbucks.getLocation().getY()));
    }

    @Test
    void test_within_box() {

        Box box = new Box(new Point(-73.99756, 40.73083), new Point(-73.988135, 40.741404));

        List<Starbucks> starbucksList = mongoTemplate.find(
                new Query(
                        Criteria.where("location")
                                .within(box)),
                Starbucks.class);

        log.info("size = {}", starbucksList.size());
        starbucksList.forEach(starbucks ->
                log.info("id = {}, (x, y) = ({}, {})",
                        starbucks.getId(),
                        starbucks.getLocation().getX(),
                        starbucks.getLocation().getY()));
    }

    @Test
    void test_near_distance() {

        List<Starbucks> starbucksList = mongoTemplate.find(
                new Query(
                        Criteria.where("location")
                                .near(new Point(-73.99171, 40.738868))
                                .maxDistance(0.01)),
                Starbucks.class);

        log.info("size = {}", starbucksList.size());
        starbucksList.forEach(starbucks ->
                log.info("id = {}, (x, y) = ({}, {})",
                        starbucks.getId(),
                        starbucks.getLocation().getX(),
                        starbucks.getLocation().getY()));
    }
}
