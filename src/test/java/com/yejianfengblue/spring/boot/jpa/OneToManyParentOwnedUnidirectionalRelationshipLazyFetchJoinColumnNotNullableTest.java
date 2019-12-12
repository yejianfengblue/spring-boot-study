package com.yejianfengblue.spring.boot.jpa;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import net.ttddyy.dsproxy.asserts.PreparedExecution;
import net.ttddyy.dsproxy.asserts.ProxyTestDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.persistence.*;

import static net.ttddyy.dsproxy.asserts.assertj.DataSourceAssertAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true"})
@Import(ProxyTestDataSourceConfig.class)
class OneToManyParentOwnedUnidirectionalRelationshipLazyFetchJoinColumnNotNullableTest {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private ProxyTestDataSource ptds;

    private Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Reset query execution logging
     */
    @BeforeEach
    void resetProxyTestDataSource() {

        ptds.reset();
    }

    @Entity
    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class Post {

        @Id
        @GeneratedValue
        private Long id;

        @EqualsAndHashCode.Include
        private String title;

        @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, orphanRemoval = true)
        @JoinColumn(name = "post_id", nullable = false)
        @ToString.Exclude
        private List<PostComment> postCommentList = new ArrayList<>();

        Post() {}

        Post(String title) { this.title = title; }
    }

    @Entity
    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class PostComment {

        @Id
        @GeneratedValue
        private Long id;

        @EqualsAndHashCode.Include
        private String review;

        PostComment() {}

        PostComment(String review) { this.review = review; }
    }

    /**
     * The child records are inserted first without the foreign key, since child entity doesn't store
     * this info (no parent ref).
     * During the collection handling phase, the foreign key column is updated then.
     */
    @Test
    void givenParentOwnedOneToManyUnidirectionalRelationship_whenSaveParentAlongWithChildren_thenChildrenAreFirstInsertedWithoutFkAndUpdateFkInSeparateUpdateQuery() {

        // when
        transactionTemplate.executeWithoutResult(status -> {

            Post post = new Post("Some post");
            post.getPostCommentList().add(
                    new PostComment("First comment")
            );
            post.getPostCommentList().add(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);
            entityManager.flush();
        });

        // then
        assertThat(ptds).hasInsertCount(3);
        assertThat(ptds).hasUpdateCount(2);
        List<String> insertOrUpdateQueryList = ptds.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("INSERT") || query.startsWith("UPDATE"))
                .collect(Collectors.toList());
        assertThat(insertOrUpdateQueryList).hasSize(5);
        assertThat(insertOrUpdateQueryList.get(0)).matches("INSERT INTO [\\w_$]*POST( \\w+)? .+");
        assertThat(insertOrUpdateQueryList.get(1)).matches("INSERT INTO [\\w_$]*POST_COMMENT( \\w+)? .+");
        assertThat(insertOrUpdateQueryList.get(2)).matches("INSERT INTO [\\w_$]*POST_COMMENT( \\w+)? .+");
        assertThat(insertOrUpdateQueryList.get(3)).matches("UPDATE [\\w_$]*POST_COMMENT( \\w+)? SET POST_ID=\\? WHERE ID=\\?");
        assertThat(insertOrUpdateQueryList.get(4)).matches("UPDATE [\\w_$]*POST_COMMENT( \\w+)? SET POST_ID=\\? WHERE ID=\\?");
    }

    /**
     * The update query sets child foreign key to null to disassociate the relationship.
     * The delete query deletes the removed child record due to orphanRemoval = true.
     */
    @Test
    void givenParentOwnedOneToManyUnidirectionalRelationshipAndParentSideLazyFetch_whenRemoveChild_thenOnlyOneDeleteStatementGetExecuted() {

        transactionTemplate.executeWithoutResult(status -> {

            // data preparation
            Post post = new Post("Some post");
            post.getPostCommentList().add(
                    new PostComment("First comment")
            );
            post.getPostCommentList().add(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);
            entityManager.flush();

            ptds.reset();  // reset query execution logging

            // when
            post.getPostCommentList().remove(0);
            entityManager.persist(post);
            entityManager.flush();

            assertThat(ptds).hasUpdateCount(0);
            assertThat(ptds).hasDeleteCount(1);
            List<String> deleteQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("DELETE"))
                    .collect(Collectors.toList());
            assertThat(deleteQueryList).hasSize(1);
            assertThat(deleteQueryList.get(0)).matches(
                    "DELETE FROM [\\w_$]*POST_COMMENT( \\w+)? WHERE ID=\\?");
        });
    }
}
