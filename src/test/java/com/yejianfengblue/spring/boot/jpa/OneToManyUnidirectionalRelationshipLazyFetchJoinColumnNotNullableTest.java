package com.yejianfengblue.spring.boot.jpa;

import lombok.Data;
import lombok.ToString;
import net.ttddyy.dsproxy.asserts.PreparedExecution;
import net.ttddyy.dsproxy.asserts.ProxyTestDataSource;
import org.hibernate.LazyInitializationException;
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
import java.util.Objects;
import java.util.stream.Collectors;

import static net.ttddyy.dsproxy.asserts.assertj.DataSourceAssertAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true"})
@Import(ProxyTestDataSourceConfig.class)
class OneToManyUnidirectionalRelationshipLazyFetchJoinColumnNotNullableTest {

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
    private static class Post implements AssertEqualityConsistencyUtil.EntityInterface {

        @Id
        @GeneratedValue
        private Long id;

        private String title;

        // Mustn't use Set because PostComment doesn't have a business key and hashCode() always return same value
        @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, orphanRemoval = true)
        @JoinColumn(name = "post_id", nullable = false)
        @ToString.Exclude
        private List<PostComment> postComments = new ArrayList<>();

        Post() {}

        Post(String title) { this.title = title; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Post)) return false;
            Post post = (Post) o;
            return Objects.equals(getId(), post.getId());
        }

        @Override
        public int hashCode() {
            return 31;
        }
    }

    @Entity
    @Data
    private static class PostComment implements AssertEqualityConsistencyUtil.EntityInterface {

        @Id
        @GeneratedValue
        private Long id;

        private String review;

        PostComment() {}

        PostComment(String review) { this.review = review; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PostComment)) return false;
            PostComment that = (PostComment) o;
            return Objects.equals(getId(), that.getId());
        }

        @Override
        public int hashCode() {
            return 31;
        }
    }

    @Test
    void testEqualityConsistency() {

        Post post = new Post("Some post");
        post.getPostComments().add(
                new PostComment("First comment")
        );
        post.getPostComments().add(
                new PostComment("Second comment")
        );
        AssertEqualityConsistencyUtil.assertEqualityConsistency(Post.class, post, transactionTemplate, entityManager);

        // because post_comment.post_id is not nullable, so skip assert equality consistency for class PostComment
    }


    @Test
    void givenOneToManyUnidirectionalRelationshipLazyFetch_whenFindOneSide_thenManySideCollectionAreNotFetched() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(status -> {

            Post post = new Post("Some post");
            post.getPostComments().add(
                    new PostComment("First comment")
            );
            post.getPostComments().add(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);
            entityManager.flush();

            return post.getId();
        });

        ptds.reset();  // reset query execution logging

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);

            // then
            assertThat(ptds).hasSelectCount(1);
            List<String> selectQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("SELECT"))
                    .collect(Collectors.toList());
            assertThat(selectQueryList).hasSize(1);
            assertThat(selectQueryList.get(0)).matches("SELECT .+ FROM [\\w_$]*POST( \\w+)? WHERE .+");
        });
    }

    /**
     * The many-side records are inserted first WITH the foreign key, even though many-side entity doesn't have
     * an one-side reference thus doesn't know the foreign key, but JPA vendor manages to provide the foreign key value
     * During the collection handling phase, the foreign key column is updated again.
     */
    @Test
    void givenOneToManyUnidirectionalRelationship_whenSaveOneSideAlongWithManySideCollection_thenManySideRecordsAreFirstInsertedWithFkAndFkIsUpdatedAgain() {

        // when
        transactionTemplate.executeWithoutResult(status -> {

            Post post = new Post("Some post");
            post.getPostComments().add(
                    new PostComment("First comment")
            );
            post.getPostComments().add(
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
        assertThat(insertOrUpdateQueryList.get(1)).matches("INSERT INTO [\\w_$]*POST_COMMENT \\([\\w, ]*POST_ID[\\w, ]*\\) .+");
        assertThat(insertOrUpdateQueryList.get(2)).matches("INSERT INTO [\\w_$]*POST_COMMENT \\([\\w, ]*POST_ID[\\w, ]*\\) .+");
        assertThat(insertOrUpdateQueryList.get(3)).matches("UPDATE [\\w_$]*POST_COMMENT( \\w+)? SET POST_ID=\\? WHERE ID=\\?");
        assertThat(insertOrUpdateQueryList.get(4)).matches("UPDATE [\\w_$]*POST_COMMENT( \\w+)? SET POST_ID=\\? WHERE ID=\\?");
    }

    /**
     * Just one delete statement deletes the removed many-side records due to orphanRemoval = true.
     */
    @Test
    void givenOneToManyUnidirectionalRelationship_whenRemoveManySide_thenOnlyOneDeleteStatementGetExecuted() {

        transactionTemplate.executeWithoutResult(status -> {

            // data preparation
            Post post = new Post("Some post");
            post.getPostComments().add(
                    new PostComment("First comment")
            );
            post.getPostComments().add(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);
            entityManager.flush();

            ptds.reset();  // reset query execution logging

            // when
            post.getPostComments().remove(0);
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

    @Test
    void givenLazyFetchUninitializedCollection_whenUseThatCollectionOutOfOriginTransaction_thenLazyInitializationException() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(status -> {

            Post post = new Post("Some post");
            post.getPostComments().add(
                    new PostComment("First comment")
            );
            post.getPostComments().add(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);
            entityManager.flush();

            return post.getId();
        });

        // given
        List<PostComment> foundPostCommentList = transactionTemplate.execute(transactionStatus -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);
            return foundPost.getPostComments();
        });

        // then
        assertThatThrownBy(() -> {
            // when
            foundPostCommentList.size();
        }).isInstanceOf(LazyInitializationException.class)
                .hasMessageContaining("no Session");
    }

    @Test
    void givenLazyFetchUninitializedCollection_whenUseThatCollectionWithinSameTransaction_thenCollectionIsFetchedInSeparatedQuery() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(status -> {

            Post post = new Post("Some post");
            post.getPostComments().add(
                    new PostComment("First comment")
            );
            post.getPostComments().add(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);
            entityManager.flush();

            return post.getId();
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);
            ptds.reset();  // reset query execution logging

            // given
            List<PostComment> postCommentList = foundPost.getPostComments();
            assertThat(ptds).hasSelectCount(0);

            // when
            postCommentList.size();  // trigger select from post_comment

            // then
            assertThat(ptds).hasSelectCount(1);
            List<String> selectQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("SELECT"))
                    .collect(Collectors.toList());
            assertThat(selectQueryList).hasSize(1);
            assertThat(selectQueryList.get(0)).matches("SELECT .+ FROM [\\w_$]*POST_COMMENT( \\w+)? WHERE .+");
        });
    }

}
