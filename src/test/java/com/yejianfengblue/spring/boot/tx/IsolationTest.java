package com.yejianfengblue.spring.boot.tx;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.persistence.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EntityScan(basePackageClasses = IsolationTest.class)
@Import(ProxyTestDataSourceConfig.class)
class IsolationTest {

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private TransactionTemplate transactionTemplate;

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

    @DisplayName("READ_UNCOMMITTED suffers from dirty read")
    @Test
    void givenTransactionWithIsolationReadUncommitted_whenOneConcurrentTransactionChangeValueButNotYetCommit_thenAnotherConcurrentTransactionReadTheUncommittedValue() throws ExecutionException, InterruptedException {

        // data preparation
        Long accountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });

        // given
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_UNCOMMITTED);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch canRead = new CountDownLatch(1);
        CountDownLatch canCommit = new CountDownLatch(1);

        // when
        // make a change, wait for another thread to read, then commit
        executorService.submit(() -> {

            transactionTemplate.executeWithoutResult(transactionStatus -> {
                Account foundAccount = entityManager.find(Account.class, accountId);
                foundAccount.setBalance(1000);
                entityManager.flush();
                canRead.countDown();
                // commit after below thread read
                try {
                    canCommit.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });

        // then
        // wait for change in the above thread, read, then signal above thread to commit
        Future<Integer> readBalance = executorService.submit(() -> {

            return transactionTemplate.execute(transactionStatus -> {
                // wait for change before read
                try {
                    canRead.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Account foundAccount = entityManager.find(Account.class, accountId);
                canCommit.countDown();
                return foundAccount.getBalance();
            });
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(1000, readBalance.get());
    }

    @DisplayName("READ_COMMITTED doesn't suffer from dirty read")
    @Test
    void givenTransactionWithIsolationReadCommitted_whenOneConcurrentTransactionChangeValueButNotYetCommit_thenAnotherConcurrentTransactionReadTheOriginalValue() throws ExecutionException, InterruptedException {

        // data preparation
        Long accountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });

        // given
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch canRead = new CountDownLatch(1);
        CountDownLatch canCommit = new CountDownLatch(1);

        // when
        // make a change, wait for another thread to read, then commit
        executorService.submit(() -> {

            transactionTemplate.executeWithoutResult(transactionStatus -> {

                Account foundAccount = entityManager.find(Account.class, accountId);
                foundAccount.setBalance(1000);
                entityManager.flush();
                canRead.countDown();
                // commit after below thread read
                try {
                    canCommit.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });

        // then-1
        // wait for change in the above thread, read, then signal above thread to commit
        Future<Integer> readBalance = executorService.submit(() -> {

            return transactionTemplate.execute(transactionStatus -> {

                // wait for change before read
                try {
                    canRead.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Account foundAccount = entityManager.find(Account.class, accountId);
                canCommit.countDown();
                return foundAccount.getBalance();
            });
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // then-2
        assertEquals(0, readBalance.get());
    }

    @DisplayName("READ_COMMITTED suffers from non-repeatable read")
    @Test
    void givenTransactionWithIsolationReadCommitted_whenAnotherConcurrentTransactionChangeValueAndCommit_thenReReadGetChangedValue() {

        // data preparation
        Long accountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });

        // given
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch canChange = new CountDownLatch(1);
        CountDownLatch canReRead = new CountDownLatch(1);

        // when
        // make a change and commit
        executorService.submit(() -> {

            transactionTemplate.executeWithoutResult(transactionStatus -> {

                Account foundAccount = entityManager.find(Account.class, accountId);
                foundAccount.setBalance(1000);
                try {
                    canChange.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                entityManager.flush();
            });
            canReRead.countDown();
        });

        // then-1
        // first read, wait another concurrent transaction change value, second read
        AtomicInteger firstReadBalance = new AtomicInteger(-1);
        AtomicInteger secondReadBalance = new AtomicInteger(-1);
        executorService.submit(() -> {

            transactionTemplate.executeWithoutResult(transactionStatus -> {

                // first read
                Account foundAccount = entityManager.find(Account.class, accountId);
                firstReadBalance.set(foundAccount.getBalance());
                canChange.countDown();

                try {
                    canReRead.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // second read (refresh the state from database)
                entityManager.refresh(foundAccount);
                secondReadBalance.set(foundAccount.getBalance());
            });
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // then-2
        assertEquals(0, firstReadBalance.get());
        assertEquals(1000, secondReadBalance.get());
    }

    @DisplayName("READ_COMMITTED suffers from phantom read")
    @Test
    void givenTransactionWithIsolationReadCommitted_whenAnotherConcurrentTransactionAddRow_thenReExecutionOfRangeQueryGetNewlyAddedRow() {

        // data preparation
        Long accountId = transactionTemplate.execute(transactionStatus -> {

            entityManager.createQuery(
                    "DELETE FROM " + Account.class.getName()
            ).executeUpdate();
            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });

        // given
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch allowAddRow = new CountDownLatch(1);
        CountDownLatch allowSecondRangeQuery = new CountDownLatch(1);
        CountDownLatch allowThirdRangeQuery = new CountDownLatch(1);

        // when
        // wait signal to add a row
        executorService.submit(() -> {

            log.info("Wait for signal to add row");
            try {
                allowAddRow.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Going to add new row");
            transactionTemplate.executeWithoutResult(transactionStatus -> {

                Account newlyAddedAccount = new Account("new account", 0);
                entityManager.persist(newlyAddedAccount);
            });
            log.info("allowSecondRangeQuery.countDown();");
            allowSecondRangeQuery.countDown();
        });

        AtomicLong firstRangeQuerySize = new AtomicLong(-1);
        AtomicLong secondRangeQuerySize = new AtomicLong(-1);
        // then-1
        // first range query, signal above thread to add a new row, second range query
        executorService.submit(() -> {

            transactionTemplate.executeWithoutResult(transactionStatus -> {
                log.info("first range query");

                firstRangeQuerySize.set((Long)
                        entityManager.createQuery(
                                "SELECT COUNT(a) FROM " + Account.class.getName() + " a"
                        ).getSingleResult()
                );

                log.info("allowAddRow.countDown();");
                allowAddRow.countDown();

                try {
                    allowSecondRangeQuery.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                entityManager.clear();

                log.info("second range query");
                secondRangeQuerySize.set((Long)
                        entityManager.createQuery(
                                "SELECT COUNT(a) FROM " + Account.class.getName() + " a"
                        ).getSingleResult()
                );
            });
            allowThirdRangeQuery.countDown();
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // then-2
        assertEquals(1, firstRangeQuerySize.get());
        assertEquals(2, secondRangeQuerySize.get());
    }

    @DisplayName("REPEATABLE_READ doesn't suffer from non-repeatable read")
    @Test
    void givenTransactionWithIsolationRepeatableRead_whenAnotherConcurrentTransactionChangeValueAndCommit_thenReReadIsStillOriginalValue() {

        // data preparation
        Long accountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });

        // given
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch canChange = new CountDownLatch(1);
        CountDownLatch canReRead = new CountDownLatch(1);

        // when
        // make a change and commit
        executorService.submit(() -> {

            transactionTemplate.executeWithoutResult(transactionStatus -> {

                Account foundAccount = entityManager.find(Account.class, accountId);
                foundAccount.setBalance(1000);
                try {
                    canChange.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                entityManager.flush();
            });
            canReRead.countDown();
        });

        // then-1
        // first read, wait another concurrent transaction change value, second read
        AtomicInteger firstReadBalance = new AtomicInteger(-1);
        AtomicInteger secondReadBalance = new AtomicInteger(-1);
        executorService.submit(() -> {

            transactionTemplate.executeWithoutResult(transactionStatus -> {

                // first read
                Account foundAccount = entityManager.find(Account.class, accountId);
                firstReadBalance.set(foundAccount.getBalance());
                canChange.countDown();

                try {
                    canReRead.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // second read (refresh the state from database)
                entityManager.refresh(foundAccount);
                secondReadBalance.set(foundAccount.getBalance());
            });
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // then-2
        assertEquals(0, firstReadBalance.get());
        assertEquals(0, secondReadBalance.get());
    }

    @DisplayName("REPEATABLE_READ prevents concurrent update to same row")
    @Test
    void givenTransactionWithIsolationRepeatableRead_whenTwoConcurrentTransactionUpdateSameRow_thenLaterThrowException() {

        // data preparation
        Long accountId = transactionTemplate.execute(transactionStatus -> {

            Account account = new Account("test", 0);
            entityManager.persist(account);
            return account.getId();
        });

        // given
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch allowFirstThreadCommit = new CountDownLatch(1);
        CountDownLatch allowSecondThreadUpdate = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();

        // when
        // make a change, flush, wait for second update, commit
        executorService.submit(() -> {

            transactionTemplate.executeWithoutResult(transactionStatus -> {

                Account foundAccount = entityManager.find(Account.class, accountId);
                log.info("foundAccount after find lock mode = {}", entityManager.getLockMode(foundAccount));
                foundAccount.setBalance(1000);
                entityManager.flush();
                log.info("foundAccount after flush mode = {}", entityManager.getLockMode(foundAccount));
                allowSecondThreadUpdate.countDown();
                try {
                    allowFirstThreadCommit.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });

        // then-1
        // wait for first update, make a change, flush, should timeout trying to lock
        executorService.submit(() -> {

            try {
                allowSecondThreadUpdate.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                transactionTemplate.executeWithoutResult(transactionStatus -> {

                    // will throw PessimisticLockException due to "Timeout trying to lock table"
                    Account foundAccount = entityManager.find(Account.class, accountId);
                    foundAccount.setBalance(1000);
                    entityManager.flush();
                });
            } catch (Exception e) {
                exception.set(e);
                throw e;
            } finally {
                allowFirstThreadCommit.countDown();
            }
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // then-2
        assertThat(exception.get()).isInstanceOf(PessimisticLockException.class);
    }
}
