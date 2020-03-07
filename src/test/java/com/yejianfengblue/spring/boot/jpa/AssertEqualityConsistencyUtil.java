package com.yejianfengblue.spring.boot.jpa;

import org.hibernate.Session;
import org.springframework.transaction.support.TransactionTemplate;

import javax.persistence.EntityManager;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

interface AssertEqualityConsistencyUtil {

    interface EntityInterface {

        Long getId();
    }

    static <T extends EntityInterface> void assertEqualityConsistency(Class<T> clazz,
                                                                         T entity,
                                                                         TransactionTemplate transactionTemplate,
                                                                         EntityManager entityManager) {

        HashSet<T> entityHashSet = new HashSet<>();

        assertFalse(entityHashSet.contains(entity));
        entityHashSet.add(entity);
        assertTrue(entityHashSet.contains(entity));

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            entityManager.persist(entity);
            entityManager.flush();
            assertTrue(entityHashSet.contains(entity),
                    "The entity is not found in the Set after it's persisted");
        });

        assertTrue(entityHashSet.contains(entity));

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            T entityProxy = entityManager.getReference(clazz, entity.getId());
            assertTrue(entityProxy.equals(entity),
                    "The entity proxy is not equal with the entity");
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            T entityProxy = entityManager.getReference(clazz, entity.getId());
            assertTrue(entity.equals(entityProxy),
                    "The entity is not equal with the entity proxy");
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            T _entity = entityManager.merge(entity);
            assertTrue(entityHashSet.contains(_entity),
                    "The entity is not found in the Set after it's merged");
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            entityManager.unwrap(Session.class).update(entity);
            assertTrue(entityHashSet.contains(entity),
                    "The entity is not found in the Set after it's reattached");
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            T _entity = entityManager.find(clazz, entity.getId());
            assertTrue(entityHashSet.contains(_entity),
                    "The entity is not found in the Set after it's loaded in a subsequent Persistence Context");
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            T _entity = entityManager.getReference(clazz, entity.getId());
            assertTrue(entityHashSet.contains(_entity),
                    "The entity is not in the Set found after it's loaded as a proxy in other Persistence Context");
        });

        T deletedEntity = transactionTemplate.execute(transactionStatus -> {

            T _entity = entityManager.getReference(clazz, entity.getId());
            entityManager.remove(_entity);
            return _entity;
        });

        assertTrue(entityHashSet.contains(deletedEntity),
                "The entity is found in not the Set even after it's deleted");
    }

}
