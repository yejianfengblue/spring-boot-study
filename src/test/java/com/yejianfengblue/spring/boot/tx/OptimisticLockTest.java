package com.yejianfengblue.spring.boot.tx;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import net.ttddyy.dsproxy.asserts.ProxyTestDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.persistence.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author yejianfengblue
 */
@SpringBootTest
@EntityScan(basePackageClasses = OptimisticLockTest.class)
@Import(ProxyTestDataSourceConfig.class)
class OptimisticLockTest {

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private ProxyTestDataSource proxyTestDataSource;

    private Logger log = LoggerFactory.getLogger(getClass());

    @Entity
    @Data
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class Account {

        @Id
        @GeneratedValue
        private Long id;

        @EqualsAndHashCode.Include
        private String accountName;

        private Integer balance;

        @Version
        private Integer version;

        Account(String accountName, Integer balance) {

            this.accountName = accountName;
            this.balance = balance;
        }
    }

    /**
     * Reset query execution logging
     */
    @BeforeEach
    void resetProxyTestDataSource() {

        proxyTestDataSource.reset();
    }

    @DisplayName("When insert, then acquire OPTIMISTIC_FORCE_INCREMENT")
    @Test
    void whenPersistNewEntity_thenOptimisticForceIncrementLockIsAcquired() {

        LockModeType lockModeType = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return entityManager.getLockMode(account);
        });

        assertEquals(LockModeType.OPTIMISTIC_FORCE_INCREMENT, lockModeType);
    }

    @DisplayName("When EntityManager.find(entity, id), then acquire OPTIMISTIC lock")
    @Test
    void whenEntityManagerFind_thenAcquireOptimisticLock() {

        // data preparation
        Long createdAccountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });
        LockModeType lockModeType = transactionTemplate.execute(status -> {

            Account account = entityManager.find(Account.class, createdAccountId);
            return entityManager.getLockMode(account);
        });
        assertEquals(LockModeType.OPTIMISTIC, lockModeType);
    }

    @DisplayName("When read acquiring OPTIMISTIC, then version NOT incremented after commit")
    @Test
    void whenEntityManagerFindAcquiringOptimisticLockAndNoUpdate_thenVersionNotIncrementedAfterCommit() {

        // data preparation
        Long createdAccountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });
        Integer afterCreateVersion = transactionTemplate.execute(status -> {

            Account account = entityManager.find(Account.class, createdAccountId, LockModeType.NONE);
            return account.getVersion();
        });
        assertEquals(0, afterCreateVersion);

        AtomicInteger afterOptimisticFindBeforeCommitVersion = new AtomicInteger();
        Account afterOptimisticFindAfterCommitAccount = transactionTemplate.execute(status -> {

            Account account = entityManager.find(Account.class, createdAccountId, LockModeType.OPTIMISTIC);
            afterOptimisticFindBeforeCommitVersion.set(account.getVersion());
            return account;
        });
        assertNotNull(afterOptimisticFindAfterCommitAccount);
        // version not incremented after commit
        assertEquals(0, afterOptimisticFindAfterCommitAccount.getVersion());

        Integer afterOptimisticFindAfterCommitVersion = transactionTemplate.execute(status -> {

            return entityManager.find(Account.class, createdAccountId, LockModeType.NONE).getVersion();
        });
        assertEquals(0, afterOptimisticFindAfterCommitVersion);
    }

    @DisplayName("When read acquiring OPTIMISTIC_FORCE_INCREMENT, then version incremented after commit")
    @Test
    void whenEntityManagerFindAcquiringOptimisticForceIncrementLockAndNoUpdate_thenVersionGetIncrementedAfterCommit() {

        // data preparation
        Long createdAccountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });
        Integer afterCreateVersion = transactionTemplate.execute(status -> {

            Account account = entityManager.find(Account.class, createdAccountId, LockModeType.NONE);
            return account.getVersion();
        });
        assertEquals(0, afterCreateVersion);

        AtomicInteger afterOptimisticForceIncrementFindBeforeCommitVersion = new AtomicInteger();
        Account afterOptimisticForceIncrementFindAfterCommitAccount = transactionTemplate.execute(status -> {

            Account account = entityManager.find(Account.class, createdAccountId, LockModeType.OPTIMISTIC_FORCE_INCREMENT);
            afterOptimisticForceIncrementFindBeforeCommitVersion.set(account.getVersion());
            return account;
        });
        // version not incremented before commit
        assertEquals(0, afterOptimisticForceIncrementFindBeforeCommitVersion.get());
        assertNotNull(afterOptimisticForceIncrementFindAfterCommitAccount);
        // version incremented after commit, the return account is the one after commit
        assertEquals(1, afterOptimisticForceIncrementFindAfterCommitAccount.getVersion());

        Integer afterOptimisticForceIncrementFindAfterCommitVersion = transactionTemplate.execute(status -> {

            return entityManager.find(Account.class, createdAccountId, LockModeType.NONE).getVersion();
        });
        assertEquals(1, afterOptimisticForceIncrementFindAfterCommitVersion);
    }

    @DisplayName("Given OPTIMISTIC_FORCE_INCREMENT, when update, then version incremented after flush")
    @Test
    void givenEntityManagerFindAcquiringOptimisticForceIncrementLock_whenUpdate_thenVersionGetIncrementedAfterFlushAndIncrementedAgainAfterCommit() {

        // data preparation
        Account createdAccount = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account;
        });

        assertNotNull(createdAccount);
        assertEquals(0, createdAccount.getVersion());

        AtomicInteger afterUpdateFlushBeforeCommitVersion = new AtomicInteger();

        Account updatedAccount = transactionTemplate.execute(status -> {

            Account account = entityManager.find(Account.class, createdAccount.getId(), LockModeType.OPTIMISTIC_FORCE_INCREMENT);
            account.setBalance(1000);
            entityManager.flush();
            afterUpdateFlushBeforeCommitVersion.set(account.getVersion());
            return account;
        });

        assertEquals(1, afterUpdateFlushBeforeCommitVersion.get());  // version + 1
        assertNotNull(updatedAccount);
        assertEquals(2, updatedAccount.getVersion());  // version + 1 again

        Integer afterUpdateVersion = transactionTemplate.execute(status -> {

            Account account = entityManager.find(Account.class, createdAccount.getId());
            return account.getVersion();
        });
        assertEquals(2, afterUpdateVersion);
    }

    @DisplayName("Given OPTIMISTIC, when update, then acquire OPTIMISTIC_FORCE_INCREMENT and version incremented after flush")
    @Test
    void givenEntityManagerFindAcquiringOptimisticLock_whenUpdate_thenAcquireOptimisticForceIncrementLockAndVersionGetIncrementedAfterFlush() {

        // data preparation
        Account createdAccount = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account;
        });

        assertNotNull(createdAccount);
        assertEquals(0, createdAccount.getVersion());

        AtomicReference<LockModeType> beforeUpdateLockMode = new AtomicReference<>();
        AtomicReference<LockModeType> afterUpdateLockMode = new AtomicReference<>();
        AtomicInteger afterUpdateFlushBeforeCommitVersion = new AtomicInteger();

        Account updatedAccount = transactionTemplate.execute(status -> {

            Account account = entityManager.find(Account.class, createdAccount.getId(), LockModeType.OPTIMISTIC);
            beforeUpdateLockMode.set(entityManager.getLockMode(account));
            account.setBalance(1000);
            entityManager.flush();
            afterUpdateLockMode.set(entityManager.getLockMode(account));
            afterUpdateFlushBeforeCommitVersion.set(account.getVersion());
            return account;
        });

        assertEquals(1, afterUpdateFlushBeforeCommitVersion.get());
        assertNotNull(updatedAccount);
        assertEquals(1, updatedAccount.getVersion());

        assertEquals(LockModeType.OPTIMISTIC, beforeUpdateLockMode.get());
        assertEquals(LockModeType.OPTIMISTIC_FORCE_INCREMENT, afterUpdateLockMode.get());
    }

    @DisplayName("Given OPTIMISTIC, when concurrent update, then later update throw OptimisticLockException")
    @Test
    void givenVersionedEntityWithOptimisticLock_whenConcurrentUpdate_thenLaterUpdateThrowOptimisticLockException () {

        // data preparation
        Long createdAccountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch allowFirstThreadRead = new CountDownLatch(1);
        CountDownLatch allowSecondThreadUpdate = new CountDownLatch(1);

        // the first thread will update entity after second thread find original entity with old version value
        executorService.submit(() -> {
            try {
                allowFirstThreadRead.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            transactionTemplate.executeWithoutResult(transactionStatus -> {

                // version = 0
                Account account = entityManager.find(Account.class, createdAccountId, LockModeType.OPTIMISTIC);
                account.setBalance(1000);
            });
            // version = 1
            allowSecondThreadUpdate.countDown();
        });

        AtomicReference<Exception> jpaException = new AtomicReference<>();
        // the second thread find entity, wait the first thread update, then update with old version value
        executorService.submit(() -> {
            transactionTemplate.executeWithoutResult(transactionStatus -> {

                // version = 0
                Account account = entityManager.find(Account.class, createdAccountId, LockModeType.OPTIMISTIC);

                allowFirstThreadRead.countDown();

                try {
                    allowSecondThreadUpdate.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // after first thread update, version should be 1
                account.setBalance(1000);

                try {
                    entityManager.flush();
                } catch (Exception e) {
                    jpaException.set(e);
                    throw e;
                }
            });
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertThat(jpaException.get()).isInstanceOf(OptimisticLockException.class);
    }
}
