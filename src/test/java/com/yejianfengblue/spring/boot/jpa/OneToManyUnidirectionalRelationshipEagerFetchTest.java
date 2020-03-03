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

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static net.ttddyy.dsproxy.asserts.assertj.DataSourceAssertAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true"})
@Import(ProxyTestDataSourceConfig.class)
class OneToManyUnidirectionalRelationshipEagerFetchTest {

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

        @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, orphanRemoval = true)
        @JoinColumn(name = "post_id")
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

    @Test
    void givenOneToManyUnidirectionalRelationshipEagerFetch_whenFindOneSide_thenManySideCollectionIsFetchedAlongInSingleQuery() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(status -> {

            Post post = new Post("Some post");
            post.getPostCommentList().add(
                    new PostComment("First comment")
            );
            post.getPostCommentList().add(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);
            entityManager.flush();

            return post.getId();
        });

        ptds.reset();  // reset query execution logging

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            // when
            Post foundPost = entityManager.find(Post.class, createdPostId);

            // then
            assertThat(ptds).hasSelectCount(1);
            List<String> selectQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("SELECT"))
                    .collect(Collectors.toList());
            assertThat(selectQueryList).hasSize(1);
            assertThat(selectQueryList.get(0)).matches(
                    "SELECT .+ " +
                            "FROM [\\w_$]*POST( [\\w_]+)? " +
                            "LEFT OUTER JOIN [\\w_$]*POST_COMMENT( [\\w_]+)? " +
                            "ON [\\w_.=]+ " +
                            "WHERE .+");
        });
    }

    /**
     * The many-side records are inserted first without the foreign key, since many-side entity doesn't have
     * an one-side reference thus doesn't know the foreign key.
     * During the collection handling phase, the foreign key column is updated then.
     */
    @Test
    void givenOneToManyUnidirectionalRelationship_whenSaveOneSideAlongWithManySideCollection_thenManySideAreFirstInsertedWithoutFkAndUpdateFkInSeparateUpdateQuery() {

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
     * Update the foreign key to null to disassociate the relationship.
     * Delete the removed record due to orphanRemoval = true.
     */
    @Test
    void givenOneToManyUnidirectionalRelationship_whenRemoveManySideObjectFromCollection_thenManySideFkIsUpdatedToNullAndThenManySideRecordsAreDeleted() {

        Long createdPostId = transactionTemplate.execute(status -> {

            // data preparation
            Post post = new Post("Some post");
            post.getPostCommentList().add(
                    new PostComment("First comment")
            );
            post.getPostCommentList().add(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);

            return post.getId();
        });

        transactionTemplate.executeWithoutResult(status -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);

            ptds.reset();  // reset query execution logging

            // when
            foundPost.getPostCommentList().remove(0);
            // persist() and flush() are optional because post will be auto saved on transaction commit
//            entityManager.persist(post);
//            entityManager.flush();
        });

        assertThat(ptds).hasUpdateCount(1);
        assertThat(ptds).hasDeleteCount(1);
        List<String> updateOrDeleteQueryList = ptds.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("UPDATE") || query.startsWith("DELETE"))
                .collect(Collectors.toList());
        assertThat(updateOrDeleteQueryList).hasSize(2);
        assertThat(updateOrDeleteQueryList.get(0)).matches(
                "UPDATE [\\w_$]*POST_COMMENT( \\w+)? SET POST_ID=NULL WHERE POST_ID=\\? AND ID=\\?");
        assertThat(updateOrDeleteQueryList.get(1)).matches(
                "DELETE FROM [\\w_$]*POST_COMMENT( \\w+)? WHERE ID=\\?");
    }

}
