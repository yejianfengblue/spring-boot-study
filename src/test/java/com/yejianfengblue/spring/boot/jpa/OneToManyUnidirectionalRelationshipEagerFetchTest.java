package com.yejianfengblue.spring.boot.jpa;

import lombok.Data;
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
import java.util.Objects;
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
    private static class Post implements AssertEqualityConsistencyUtil.EntityInterface {

        @Id
        @GeneratedValue
        private Long id;

        private String title;

        // Mustn't use Set because PostComment doesn't have a business key and hashCode() always return same value
        @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, orphanRemoval = true)
        @JoinColumn(name = "post_id")
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

        PostComment postComment = new PostComment("Test comment");
        AssertEqualityConsistencyUtil.assertEqualityConsistency(PostComment.class, postComment, transactionTemplate, entityManager);
    }

    @Test
    void givenOneToManyUnidirectionalRelationshipEagerFetch_whenFindOneSide_thenManySideCollectionIsFetchedAlongInSingleQuery() {

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
            post.getPostComments().add(
                    new PostComment("First comment")
            );
            post.getPostComments().add(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);

            return post.getId();
        });

        transactionTemplate.executeWithoutResult(status -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);

            ptds.reset();  // reset query execution logging

            // when
            foundPost.getPostComments().remove(0);
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
