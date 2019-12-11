package com.yejianfengblue.spring.boot.jpa;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.persistence.*;
import javax.transaction.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
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
    void givenNewCreatedEntity_whenDetachAndFindAgain_thenNotSameObject() {

        // given
        MyEntity myEntity = new MyEntity();
        myEntity.setMyColumn("dummy");

        entityManager.persist(myEntity);

        // when
        entityManager.detach(myEntity);
        MyEntity foundMyEntity = entityManager.find(MyEntity.class, myEntity.getId());

        // then
        assertThat(foundMyEntity).isNotSameAs(myEntity);
    }
}
