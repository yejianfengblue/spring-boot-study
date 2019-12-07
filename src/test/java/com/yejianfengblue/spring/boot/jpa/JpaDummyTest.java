package com.yejianfengblue.spring.boot.jpa;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;

import javax.persistence.*;
import javax.transaction.Transactional;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author yejianfengblue
 */
@SpringBootTest
@Transactional
class JpaDummyTest {

    @Autowired
    private EntityManager entityManager;

    private Logger log = LoggerFactory.getLogger(getClass());

    @Entity
    @Getter
    @Setter
    @ToString
    private static class MyEntity {

        @Id
        @GeneratedValue(strategy = GenerationType.SEQUENCE)
        private Long id;

        private String myColumn;
    }

    @Test
    void givenNewEntityObject_whenUseEntityManagerToPersist_thenIdNotNull() {

        // given
        MyEntity myEntity = new MyEntity();
        myEntity.setMyColumn("dummy");

        log.info("myEntity before persist = {}", myEntity);

        // when
        entityManager.persist(myEntity);

        log.info("myEntity after persist = {}", myEntity);

        // then
        assertNotNull(myEntity.getId());
    }

    @Test
    void givenHibernatePropertyJdbcBatchSizeIsSet_whenPersistMultipleEntities_thenBatchInsert(
            @Value("${spring.jpa.properties.hibernate.jdbc.batch_size}") Integer batchSize) {

        assertNotNull(batchSize);
        assertNotEquals(0, batchSize);

        for (long i = 1; i <= 11; i++) {

            MyEntity myEntity = new MyEntity();
            myEntity.setMyColumn("dummy " + i);
            entityManager.persist(myEntity);
        }
        entityManager.flush();

        // logging like below
//        Name:MyDS, Connection:6, Time:2, Success:True
//        Type:Prepared, Batch:True, QuerySize:1, BatchSize:5
//        Query:["/* insert com.yejianfengblue.spring.boot.jpa.JpaDummyTest$MyEntity */ insert into jpa_dummy_test$my_entity (my_column, id) values (?, ?)"]
//        Params:[(dummy 1,1),(dummy 2,2),(dummy 3,3),(dummy 4,4),(dummy 5,5)]
    }
}
