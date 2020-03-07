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
 * The best way to map a @OneToMany association is to rely on the @ManyToOne side to propagate all entity state changes
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true"})
@Import(ProxyTestDataSourceConfig.class)
class OneToManyBidirectionalRelationshipLazyFetchTest {

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

    // assume post title is unique, so we use it as the business key
    @Entity
    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class Post implements AssertEqualityConsistencyUtil.EntityInterface {

        @Id
        @GeneratedValue
        private Long id;

        @EqualsAndHashCode.Include
        private String title;

        @OneToMany(
                mappedBy = "post",
                cascade = CascadeType.ALL,
                fetch = FetchType.LAZY,
                orphanRemoval = true)
        @ToString.Exclude
        private List<PostComment> postComments = new ArrayList<>();

        Post() {}

        Post(String title) { this.title = title; }

        void addPostComment(PostComment postComment) {

            postComment.setPost(this);
            postComments.add(postComment);
        }

        void removePostComment(PostComment postComment) {

            postComments.remove(postComment);
            postComment.setPost(null);
        }
    }

    // assume review content must be unique, so we use review and post as business key
    @Entity
    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class PostComment implements AssertEqualityConsistencyUtil.EntityInterface {

        @Id
        @GeneratedValue
        private Long id;

        @EqualsAndHashCode.Include
        private String review;

        @ManyToOne(fetch = FetchType.EAGER, optional = false)
        @JoinColumn(name = "post_id")
        @EqualsAndHashCode.Include
        private Post post;

        PostComment() {}

        PostComment(String review) { this.review = review; }
    }

    @Test
    void testEqualityConsistency() {

        Post post = new Post("Some post");
        post.addPostComment(
                new PostComment("First comment")
        );
        post.addPostComment(
                new PostComment("Second comment")
        );
        AssertEqualityConsistencyUtil.assertEqualityConsistency(Post.class, post, transactionTemplate, entityManager);

        // because post_comment.post_id is not nullable, so skip assert equality consistency for class PostComment
    }

    @Test
    void givenOneToManyAndManyToOneBidirectionalRelationshipAndManyToOneSideHasJoinColumn_whenSaveParentAlongWithChildren_thenChildrenAreInsertedWithFk() {

        // when
        transactionTemplate.executeWithoutResult(status -> {

            Post post = new Post("Some post");
            post.addPostComment(
                    new PostComment("First comment")
            );
            post.addPostComment(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);
            entityManager.flush();
        });

        // then
        assertThat(ptds).hasInsertCount(3);
        assertThat(ptds).hasUpdateCount(0);
        List<String> insertQueryList = ptds.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("INSERT"))
                .collect(Collectors.toList());
        assertThat(insertQueryList).hasSize(3);
        assertThat(insertQueryList.get(0)).matches("INSERT INTO [\\w_$]*POST( \\w+)? .+");
        assertThat(insertQueryList.get(1)).matches("INSERT INTO [\\w_$]*POST_COMMENT \\([\\w, ]*POST_ID[\\w, ]*\\) .+");
        assertThat(insertQueryList.get(2)).matches("INSERT INTO [\\w_$]*POST_COMMENT \\([\\w, ]*POST_ID[\\w, ]*\\) .+");
    }

    @Test
    void givenOneToManyAndManyToOneBidirectionalRelationshipAndManyToOneSideHasJoinColumn_whenRemoveChild_thenOnlyOneDeleteStatementGetExecutedToDeleteChild() {

        transactionTemplate.executeWithoutResult(status -> {

            // data preparation
            Post post = new Post("Some post");
            post.addPostComment(
                    new PostComment("First comment")
            );
            post.addPostComment(
                    new PostComment("Second comment")
            );

            entityManager.persist(post);
            entityManager.flush();

            ptds.reset();  // reset query execution logging

            // when
            post.removePostComment(post.getPostComments().get(0));
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
