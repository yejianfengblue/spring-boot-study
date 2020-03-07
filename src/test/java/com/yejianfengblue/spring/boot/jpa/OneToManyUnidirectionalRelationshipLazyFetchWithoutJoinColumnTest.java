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
import java.util.*;
import java.util.stream.Collectors;

import static net.ttddyy.dsproxy.asserts.assertj.DataSourceAssertAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true"})
@Import(ProxyTestDataSourceConfig.class)
class OneToManyUnidirectionalRelationshipLazyFetchWithoutJoinColumnTest {

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
        public int hashCode() { return 31; }
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
        public int hashCode() { return 31; }
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

    /**
     * The many-side records are inserted first without the foreign key, since many-side entity doesn't have
     * an one-side reference thus doesn't know the foreign key.
     * During the collection handling phase, the foreign key column is updated then.
     */
    @Test
    void givenOneToManyUnidirectionalRelationshipWithoutJoinColumn_whenSaveOneSideAlongWithManySideCollection_thenManySideAreFirstInsertedWithoutFkColumnAndTheRelationshipIsMaintainedInThirdJoinTable() {

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
        assertThat(ptds).hasInsertCount(5);
        assertThat(ptds).hasUpdateCount(0);
        List<String> insertOrUpdateQueryList = ptds.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("INSERT"))
                .collect(Collectors.toList());
        assertThat(insertOrUpdateQueryList).hasSize(5);
        assertThat(insertOrUpdateQueryList.get(0)).matches("INSERT INTO [\\w_$]*POST( \\w+)? .+");
        assertThat(insertOrUpdateQueryList.get(1)).matches("INSERT INTO [\\w_$]*POST_COMMENT( \\w+)? .+");
        assertThat(insertOrUpdateQueryList.get(2)).matches("INSERT INTO [\\w_$]*POST_COMMENT( \\w+)? .+");
        assertThat(insertOrUpdateQueryList.get(3)).matches("INSERT INTO [\\w_$]*POST_POST_COMMENTS( \\w+)? .+");
        assertThat(insertOrUpdateQueryList.get(4)).matches("INSERT INTO [\\w_$]*POST_POST_COMMENTS( \\w+)? .+");
    }

    /**
     * Update the foreign key to null to disassociate the relationship.
     * Delete the removed record due to orphanRemoval = true.
     */
    @Test
    void givenOneToManyUnidirectionalRelationship_whenRemoveManySideObjectFromCollection_thenAllRowsWithOneSideIdAreDeletedFromJoinTable_thenDeleteManySideRow_thenJoinTableRowsOfRemainingManySideAreInsertedBack() {

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
        });

        // then
        assertThat(ptds).hasUpdateCount(0);
        assertThat(ptds).hasDeleteCount(2);
        assertThat(ptds).hasInsertCount(1);

        List<String> deleteQueryList = ptds.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("DELETE"))
                .collect(Collectors.toList());
        assertThat(deleteQueryList).hasSize(2);
        assertThat(deleteQueryList.get(0)).matches(
                "DELETE FROM [\\w_$]*POST_POST_COMMENTS( \\w+)? WHERE [\\w_$]*POST_ID=\\?");
        assertThat(deleteQueryList.get(1)).matches(
                "DELETE FROM [\\w_$]*POST_COMMENT( \\w+)? WHERE ID=\\?");

        List<String> insertQueryList = ptds.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("INSERT"))
                .collect(Collectors.toList());
        assertThat(insertQueryList.get(0)).matches(
                "INSERT INTO [\\w_$]*POST_POST_COMMENTS( \\w+)? .+");
    }

}
