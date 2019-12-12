package com.yejianfengblue.spring.boot.jpa;

import lombok.Data;
import lombok.EqualsAndHashCode;
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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.persistence.*;

import static net.ttddyy.dsproxy.asserts.assertj.DataSourceAssertAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true"})
@Import(ProxyTestDataSourceConfig.class)
class OneToManyParentOwnedUnidirectionalRelationshipLazyFetchTest {

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
    void givenParentOwnedOneToManyUnidirectionalRelationshipLazyFetch_whenFindParent_thenChildrenAreNotFetched() {

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

    @Test
    void givenLazyFetchUninitializedCollection_whenUseThatCollectionOutOfOriginTransaction_thenLazyInitializationException() {

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

        // given
        List<PostComment> foundPostCommentList = transactionTemplate.execute(transactionStatus -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);
            return foundPost.getPostCommentList();
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

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);
            ptds.reset();  // reset query execution logging

            // given
            List<PostComment> postCommentList = foundPost.getPostCommentList();
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
    void givenParentOwnedOneToManyUnidirectionalRelationshipAndParentSideLazyFetch_whenRemoveChild_thenChildFkIsSetToNullInUpdateQueryAndDeletedInDeleteQuery() {

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
        });
    }
}
