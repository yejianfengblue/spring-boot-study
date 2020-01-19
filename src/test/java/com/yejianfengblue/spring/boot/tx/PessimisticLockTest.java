package com.yejianfengblue.spring.boot.tx;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import net.ttddyy.dsproxy.asserts.PreparedExecution;
import net.ttddyy.dsproxy.asserts.ProxyTestDataSource;
import net.ttddyy.dsproxy.asserts.assertj.DataSourceAssertAssertions;
import org.assertj.core.api.Assertions;
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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.persistence.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author yejianfengblue
 */
@SpringBootTest
@EntityScan(basePackageClasses = PessimisticLockTest.class)
@Import(ProxyTestDataSourceConfig.class)
class PessimisticLockTest {

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

        Account(String accountName, Integer balance) {

            this.accountName = accountName;
            this.balance = balance;
        }
    }

    @Entity
    @Data
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class VersionedAccount {

        @Id
        @GeneratedValue
        private Long id;

        @EqualsAndHashCode.Include
        private String accountName;

        private Integer balance;

        @Version
        private Integer version;

        VersionedAccount(String accountName, Integer balance) {

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

    @DisplayName("When find and acquire PESSIMISTIC_READ, then select for update")
    @Test
    void whenEntityManagerFindAndAcquirePessimisticReadLock_thenHibernateIssuesSelectForUpdateStatement() {

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            // given
            Account foundAccount = entityManager.find(Account.class, 0L, LockModeType.PESSIMISTIC_READ);
        });

        DataSourceAssertAssertions.assertThat(proxyTestDataSource).hasSelectCount(1);
        List<String> selectQueryList = proxyTestDataSource.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("SELECT"))
                .collect(Collectors.toList());
        assertThat(selectQueryList).hasSize(1);
        Assertions.assertThat(selectQueryList.get(0)).matches(
                "SELECT .+ " +
                        "FROM [\\w_$]*ACCOUNT( [\\w_]+)? " +
                        "WHERE [\\w_\\.]*ID=\\? " +
                        "FOR UPDATE");
    }

    @DisplayName("When find and acquire PESSIMISTIC_WRITE, then select for update")
    @Test
    void whenEntityManagerFindAndAcquirePessimisticWriteLock_thenHibernateIssuesSelectForUpdateStatement() {

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            // given
            Account foundAccount = entityManager.find(Account.class, 0L, LockModeType.PESSIMISTIC_WRITE);
        });

        DataSourceAssertAssertions.assertThat(proxyTestDataSource).hasSelectCount(1);
        List<String> selectQueryList = proxyTestDataSource.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("SELECT"))
                .collect(Collectors.toList());
        assertThat(selectQueryList).hasSize(1);
        Assertions.assertThat(selectQueryList.get(0)).matches(
                "SELECT .+ " +
                        "FROM [\\w_$]*ACCOUNT( [\\w_]+)? " +
                        "WHERE [\\w_\\.]*ID=\\? " +
                        "FOR UPDATE");
    }

    @DisplayName("Given Tx1 PESSIMISTIC_WRITE, when Tx2 PESSIMISTIC_WRITE, then PessimisticException")
    @Test
    void givenOneTransactionPessimisticWriteLockingOneRow_whenAnotherConcurrentTransactionAcquirePessimisticWriteLock_thenPessimisticException() {

        // data preparation
        Long accountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });

        proxyTestDataSource.reset();

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch allowSecondThreadLock = new CountDownLatch(1);
        CountDownLatch allowFirstThreadCommit = new CountDownLatch(1);
        AtomicReference<Exception> jpaException = new AtomicReference<>();

        executorService.submit(() -> {

            transactionTemplate.executeWithoutResult(transactionStatus -> {

                // given
                Account foundAccount = entityManager.find(Account.class, accountId, LockModeType.PESSIMISTIC_WRITE);
                allowSecondThreadLock.countDown();
                try {
                    allowFirstThreadCommit.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });

        executorService.submit(() -> {

            try {
                allowSecondThreadLock.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            transactionTemplate.executeWithoutResult(transactionStatus -> {
                try {
                    Account foundAccount = entityManager.find(Account.class, accountId, LockModeType.PESSIMISTIC_WRITE);
                }
                catch (Exception e) {
                    jpaException.set(e);
                    throw e;
                } finally {
                    allowFirstThreadCommit.countDown();
                }
            });

        });

        executorService.shutdown();
        try {
            // original test running h2 lock timeout is 6s, so 10s suffices here
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertThat(jpaException.get()).isInstanceOf(PessimisticLockException.class);

        DataSourceAssertAssertions.assertThat(proxyTestDataSource).hasSelectCount(2);
        List<String> selectQueryList = proxyTestDataSource.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("SELECT"))
                .collect(Collectors.toList());
        assertThat(selectQueryList).hasSize(2);
        Assertions.assertThat(selectQueryList.get(0)).matches(
                "SELECT .+ " +
                        "FROM [\\w_$]*ACCOUNT( [\\w_]+)? " +
                        "WHERE [\\w_\\.]*ID=\\? " +
                        "FOR UPDATE");
        Assertions.assertThat(selectQueryList.get(1)).matches(
                "SELECT .+ " +
                        "FROM [\\w_$]*ACCOUNT( [\\w_]+)? " +
                        "WHERE [\\w_\\.]*ID=\\? " +
                        "FOR UPDATE");
    }

    @DisplayName("Given Tx1 PESSIMISTIC_WRITE, when Tx2 update, then PessimisticException")
    @Test
    void givenOneTransactionPessimisticWriteLockingOneRow_whenAnotherConcurrentTransactionAcquireNoneLockAndThenUpdate_thenPessimisticException() {

        // data preparation
        Long accountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });

        proxyTestDataSource.reset();

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch allowSecondThreadLock = new CountDownLatch(1);
        CountDownLatch allowFirstThreadCommit = new CountDownLatch(1);
        AtomicReference<Exception> jpaException = new AtomicReference<>();

        executorService.submit(() -> {

            transactionTemplate.executeWithoutResult(transactionStatus -> {

                // given
                Account foundAccount = entityManager.find(Account.class, accountId, LockModeType.PESSIMISTIC_WRITE);
                allowSecondThreadLock.countDown();
                try {
                    allowFirstThreadCommit.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });

        executorService.submit(() -> {

            try {
                allowSecondThreadLock.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            transactionTemplate.executeWithoutResult(transactionStatus -> {
                Account foundAccount = entityManager.find(Account.class, accountId, LockModeType.NONE);
                foundAccount.setBalance(1000);
                // update will fail coz first thread pessimistic lock this entity
                try {
                    entityManager.flush();
                }
                catch (Exception e) {
                    jpaException.set(e);
                    throw e;
                } finally {
                    allowFirstThreadCommit.countDown();
                }
            });

        });

        executorService.shutdown();
        try {
            // original test running h2 lock timeout is 6s, so 10s suffices here
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertThat(jpaException.get()).isInstanceOf(PessimisticLockException.class);

        DataSourceAssertAssertions.assertThat(proxyTestDataSource).hasSelectCount(2);
        DataSourceAssertAssertions.assertThat(proxyTestDataSource).hasUpdateCount(1);
        List<String> selectQueryList = proxyTestDataSource.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("SELECT"))
                .collect(Collectors.toList());
        assertThat(selectQueryList).hasSize(2);
        Assertions.assertThat(selectQueryList.get(0)).matches(
                "SELECT .+ " +
                        "FROM [\\w_$]*ACCOUNT( [\\w_]+)? " +
                        "WHERE [\\w_\\.]*ID=\\? " +
                        "FOR UPDATE");
        Assertions.assertThat(selectQueryList.get(1)).matches(
                "SELECT .+ " +
                        "FROM [\\w_$]*ACCOUNT( [\\w_]+)? " +
                        "WHERE [\\w_\\.]*ID=\\?");
        List<String> updateQueryList = proxyTestDataSource.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("UPDATE"))
                .collect(Collectors.toList());
        assertThat(updateQueryList).hasSize(1);
        Assertions.assertThat(updateQueryList.get(0)).matches(
                "UPDATE [\\w_$]*ACCOUNT( [\\w_]+)? " +
                        "SET .+ " +
                        "WHERE [\\w_\\.]*ID=\\?");
    }

    @DisplayName("When read acquiring PESSIMISTIC_FORCE_INCREMENT, then version incremented after find")
    @Test
    void whenEntityManagerFindAcquiringPessimisticForceIncrementLockAndNoUpdate_thenVersionGetIncrementedAfterFind() {

        // data preparation
        Long createdAccountId = transactionTemplate.execute(transactionStatus -> {

            VersionedAccount account = new VersionedAccount("test", 0);
            entityManager.persist(account);
            return account.getId();
        });
        Integer afterCreateVersion = transactionTemplate.execute(status -> {

            VersionedAccount account = entityManager.find(VersionedAccount.class, createdAccountId, LockModeType.NONE);
            return account.getVersion();
        });
        assertEquals(0, afterCreateVersion);

        AtomicInteger afterOptimisticForceIncrementFindBeforeCommitVersion = new AtomicInteger();
        VersionedAccount afterOptimisticForceIncrementFindAfterCommitAccount = transactionTemplate.execute(status -> {

            VersionedAccount account = entityManager.find(VersionedAccount.class, createdAccountId, LockModeType.PESSIMISTIC_FORCE_INCREMENT);
            afterOptimisticForceIncrementFindBeforeCommitVersion.set(account.getVersion());
            return account;
        });
        // version not incremented before commit
        assertEquals(1, afterOptimisticForceIncrementFindBeforeCommitVersion.get());
        assertNotNull(afterOptimisticForceIncrementFindAfterCommitAccount);
        // version incremented after commit, the return account is the one after commit
        assertEquals(1, afterOptimisticForceIncrementFindAfterCommitAccount.getVersion());

        Integer afterOptimisticForceIncrementFindAfterCommitVersion = transactionTemplate.execute(status -> {

            return entityManager.find(VersionedAccount.class, createdAccountId, LockModeType.NONE).getVersion();
        });
        assertEquals(1, afterOptimisticForceIncrementFindAfterCommitVersion);
    }
}
