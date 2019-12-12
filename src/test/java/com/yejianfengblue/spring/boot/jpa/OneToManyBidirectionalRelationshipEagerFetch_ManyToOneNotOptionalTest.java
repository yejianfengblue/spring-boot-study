package com.yejianfengblue.spring.boot.jpa;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import net.ttddyy.dsproxy.asserts.PreparedExecution;
import net.ttddyy.dsproxy.asserts.ProxyTestDataSource;
import org.assertj.core.api.Assertions;
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

/**
 * The best way to map a @OneToMany association is to rely on the @ManyToOne side to propagate all entity state changes
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true"})
@Import(ProxyTestDataSourceConfig.class)
class OneToManyBidirectionalRelationshipEagerFetch_ManyToOneNotOptionalTest {

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

        @OneToMany(
                mappedBy = "post",
                cascade = CascadeType.ALL,
                fetch = FetchType.EAGER,
                orphanRemoval = true)
        @ToString.Exclude
        private List<PostComment> postCommentList = new ArrayList<>();

        Post() {
        }

        Post(String title) {
            this.title = title;
        }

        void addPostComment(PostComment postComment) {

            postCommentList.add(postComment);
            postComment.setPost(this);
        }

        void removePostComment(PostComment postComment) {

            postCommentList.remove(postComment);
            postComment.setPost(null);
        }
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

        @ManyToOne(fetch = FetchType.EAGER, optional = false)
        @JoinColumn(name = "post_id")
        private Post post;

        PostComment() {
        }

        PostComment(String review) {
            this.review = review;
        }
    }

    @Test
    void givenOneToManySideEagerFetchChildCollection_whenFindParent_thenChildCollectionIsFetchedAlongInSingleQuery() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(status -> {

            Post post = new Post("Some post");
            post.addPostComment(
                    new PostComment("First comment")
            );
            post.addPostComment(
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
            Assertions.assertThat(selectQueryList).hasSize(1);
            Assertions.assertThat(selectQueryList.get(0)).matches(
                    "SELECT .+ " +
                            "FROM [\\w_$]*POST( [\\w_]+)? " +
                            "LEFT OUTER JOIN [\\w_$]*POST_COMMENT( [\\w_]+)? " +
                            "ON [\\w_.=]+ " +
                            "WHERE .+");
        });
    }

    @Test
    void givenManyToOneSideEagerFetch_whenFindChild_thenParentIsFetchedAlongInSingleQuery() {

        // data preparation
        Long createdPostCommentId = transactionTemplate.execute(status -> {

            Post post = new Post("Some post");
            post.addPostComment(
                    new PostComment("First comment")
            );

            entityManager.persist(post);
            entityManager.flush();

            return post.getId();
        });

        ptds.reset();  // reset query execution logging

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            // when
            PostComment foundPostComment = entityManager.find(PostComment.class, createdPostCommentId);

            // then
            assertThat(ptds).hasSelectCount(1);
            List<String> selectQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("SELECT"))
                    .collect(Collectors.toList());
            Assertions.assertThat(selectQueryList).hasSize(1);
            Assertions.assertThat(selectQueryList.get(0)).matches(
                    "SELECT .+ " +
                            "FROM [\\w_$]*POST_COMMENT( [\\w_]+)? " +
                            "INNER JOIN [\\w_$]*POST( [\\w_]+)? " +
                            "ON [\\w_.=]+ " +
                            "WHERE .+");
        });
    }
}
