package com.yejianfengblue.spring.boot.jpa;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.ttddyy.dsproxy.asserts.BaseQueryExecution;
import net.ttddyy.dsproxy.asserts.PreparedBatchExecution;
import net.ttddyy.dsproxy.asserts.PreparedBatchExecutionEntry;
import net.ttddyy.dsproxy.asserts.ProxyTestDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.persistence.*;
import javax.transaction.Transactional;

import static net.ttddyy.dsproxy.asserts.assertj.DataSourceAssertAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author yejianfengblue
 */
@SpringBootTest(properties = "spring.jpa.properties.hibernate.jdbc.batch_size=5")
@Import(ProxyTestDataSourceConfig.class)
@Transactional
class HibernateBatchTest {

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private ProxyTestDataSource ptds;

    private Logger log = LoggerFactory.getLogger(getClass());

    @BeforeEach
    public void resetProxyTestDataSource() {

        ptds.reset();
    }

    @Entity
    @Getter
    @Setter
    @ToString
    private static class HibernateBatchTestEntity {

        @Id
        @GeneratedValue(strategy = GenerationType.SEQUENCE)
        private Long id;

        private String someColumn;
    }

    @Test
    void givenHibernatePropertyJdbcBatchSizeIsSet_whenPersistMultipleEntities_thenBatchInsert(
            @Value("${spring.jpa.properties.hibernate.jdbc.batch_size}") Integer batchSize) {

        assertNotNull(batchSize);
        assertNotEquals(0, batchSize);

        for (long i = 1; i <= 11; i++) {

            HibernateBatchTestEntity hibernateBatchTestEntity = new HibernateBatchTestEntity();
            hibernateBatchTestEntity.setSomeColumn("dummy " + i);
            entityManager.persist(hibernateBatchTestEntity);
        }
        entityManager.flush();

        log.info("ptds.getBatchPrepareds() = {}", ptds.getBatchPrepareds().stream()
                        .map(PreparedBatchExecution::getQuery)
                        .collect(Collectors.toList()));

        assertThat(ptds).hasBatchPreparedCount(3);  // 3 batch insert
        assertThat(ptds).hasInsertCount(3);  //  3 batch insert
        assertThat(ptds).hasPreparedCount(11);  // 11 call next value for sequence
        assertThat(ptds).hasPreparedOrBatchPreparedCount(14);  //  3 batch insert + 11 sequence
        assertThat(ptds).hasTotalQueryCount(14);  // 3 batch insert + 11 sequence

        // assert the 3 batch insert
        List<PreparedBatchExecution> batchPrepareds = ptds.getBatchPrepareds();
        assertEquals(3, batchPrepareds.size());
        assertTrue(batchPrepareds.stream()
                .allMatch(BaseQueryExecution::isSuccess));
        assertTrue(batchPrepareds.stream()
                .allMatch(BaseQueryExecution::isBatch));
        // assert the batch insert query
        assertTrue(batchPrepareds.stream()
                .allMatch(batchPrepared -> batchPrepared.getQuery()
                        .equals("insert into hibernate_batch_test$hibernate_batch_test_entity (some_column, id) values (?, ?)")));
        List<Map<Integer, Object>> batchInsertParams = batchPrepareds.stream()
                .map(PreparedBatchExecution::getPrepareds)
                .flatMap(preparedBatchExecutionEntries -> preparedBatchExecutionEntries.stream()
                        .map(PreparedBatchExecutionEntry::getSetParamsByIndex))
                .collect(Collectors.toList());
        assertThat(batchInsertParams).isEqualTo(List.of(
                Map.of(1, "dummy 1", 2, 1L),
                Map.of(1, "dummy 2", 2, 2L),
                Map.of(1, "dummy 3", 2, 3L),
                Map.of(1, "dummy 4", 2, 4L),
                Map.of(1, "dummy 5", 2, 5L),
                Map.of(1, "dummy 6", 2, 6L),
                Map.of(1, "dummy 7", 2, 7L),
                Map.of(1, "dummy 8", 2, 8L),
                Map.of(1, "dummy 9", 2, 9L),
                Map.of(1, "dummy 10", 2, 10L),
                Map.of(1, "dummy 11", 2, 11L)));

        // assert first batch insert
        PreparedBatchExecution batchInsert0 = batchPrepareds.get(0);
        assertThat(batchInsert0)
                .query()
                .isEqualToIgnoringCase("insert into hibernate_batch_test$hibernate_batch_test_entity (some_column, id) values (?, ?)");
        assertThat(batchInsert0)
                .hasBatchSize(5);
        assertThat(batchInsert0)
                .isSuccess();
        List<Map<Integer, Object>> batchInsert0Params = batchInsert0
                .getPrepareds().stream()
                .map(PreparedBatchExecutionEntry::getSetParamsByIndex)
                .collect(Collectors.toList());
        assertThat(batchInsert0Params).isEqualTo(List.of(
                Map.of(1, "dummy 1", 2, 1L),
                Map.of(1, "dummy 2", 2, 2L),
                Map.of(1, "dummy 3", 2, 3L),
                Map.of(1, "dummy 4", 2, 4L),
                Map.of(1, "dummy 5", 2, 5L)));

        // assert first batch insert
        PreparedBatchExecution batchInsert1 = batchPrepareds.get(1);
        assertThat(batchInsert1)
                .query()
                .isEqualToIgnoringCase("insert into hibernate_batch_test$hibernate_batch_test_entity (some_column, id) values (?, ?)");
        assertThat(batchInsert1)
                .hasBatchSize(5);
        assertThat(batchInsert1)
                .isSuccess();
        List<Map<Integer, Object>> batchInsert1Params = batchInsert1
                .getPrepareds().stream()
                .map(PreparedBatchExecutionEntry::getSetParamsByIndex)
                .collect(Collectors.toList());
        assertThat(batchInsert1Params).isEqualTo(List.of(
                Map.of(1, "dummy 6", 2, 6L),
                Map.of(1, "dummy 7", 2, 7L),
                Map.of(1, "dummy 8", 2, 8L),
                Map.of(1, "dummy 9", 2, 9L),
                Map.of(1, "dummy 10", 2, 10L)));

        // assert first batch insert
        PreparedBatchExecution batchInsert2 = batchPrepareds.get(2);
        assertThat(batchInsert2)
                .query()
                .isEqualToIgnoringCase("insert into hibernate_batch_test$hibernate_batch_test_entity (some_column, id) values (?, ?)");
        assertThat(batchInsert2)
                .hasBatchSize(1);
        assertThat(batchInsert2)
                .isSuccess();
        List<Map<Integer, Object>> batchInsert2Params = batchInsert2
                .getPrepareds().stream()
                .map(PreparedBatchExecutionEntry::getSetParamsByIndex)
                .collect(Collectors.toList());
        assertThat(batchInsert2Params).isEqualTo(List.of(
                Map.of(1, "dummy 11", 2, 11L)));
    }
}
