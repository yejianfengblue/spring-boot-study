package com.yejianfengblue.spring.boot.jpa;

import lombok.Data;
import lombok.EqualsAndHashCode;
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
import java.util.List;
import java.util.stream.Collectors;

import static net.ttddyy.dsproxy.asserts.assertj.DataSourceAssertAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * If in a one-to-many relationship the number of many-side records are extremely large,
 * and since no way to limit the size of a @OneToMany collection (pagination is impossible),
 * it would cause performance issue.
 * So in reality, @OneToMany is practical only when "many" means "few".
 * Consider maybe @ManyToOne is just enough
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true"})
@Import(ProxyTestDataSourceConfig.class)
class ManyToOneUnidirectionalRelationshipLazyFetchTest {

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

        @ManyToOne(fetch = FetchType.LAZY, optional = false)
        @JoinColumn(name = "post_id")
        private Post post;

        PostComment() {}

        PostComment(String review) { this.review = review; }
    }

    @Test
    void givenManyToOneUnidirectionalRelationship_oneSideEntityMustBePersistedBeforePersistManySideEntity() {

        transactionTemplate.executeWithoutResult(status -> {

            Post post = new Post("Some post");

            PostComment firstComment = new PostComment("First comment");
            firstComment.setPost(post);

            PostComment secondComment = new PostComment("Second comment");
            secondComment.setPost(post);

            entityManager.persist(post);
            entityManager.persist(firstComment);
            entityManager.persist(secondComment);
        });

        assertThat(ptds).hasInsertCount(3);
        assertThat(ptds).hasUpdateCount(0);
        List<String> insertQueryList = ptds.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("INSERT"))
                .collect(Collectors.toList());
        assertThat(insertQueryList).hasSize(3);
        assertThat(insertQueryList.get(0)).matches("INSERT INTO [\\w_$]*POST( \\w+)? .+");
        assertThat(insertQueryList.get(1)).matches("INSERT INTO [\\w_$]*POST_COMMENT( \\w+)? .+");
        assertThat(insertQueryList.get(2)).matches("INSERT INTO [\\w_$]*POST_COMMENT( \\w+)? .+");
    }

    @Test
    void givenManyToOneUnidirectionalRelationshipLazyFetch_whenFindManySideEntity_thenOneSideIsNotAutoFetched() {

        Long createdFirstCommentId = transactionTemplate.execute(status -> {

            Post post = new Post("Some post");

            PostComment firstComment = new PostComment("First comment");
            firstComment.setPost(post);

            PostComment secondComment = new PostComment("Second comment");
            secondComment.setPost(post);

            entityManager.persist(post);
            entityManager.persist(firstComment);
            entityManager.persist(secondComment);

            return firstComment.getId();
        });

        ptds.reset();  // reset query execution logging

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            entityManager.find(PostComment.class, createdFirstCommentId);
        });

        // then
        assertThat(ptds).hasSelectCount(1);
        List<String> selectQueryList = ptds.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("SELECT"))
                .collect(Collectors.toList());
        assertThat(selectQueryList).hasSize(1);
        assertThat(selectQueryList.get(0)).matches("SELECT .+ FROM [\\w_$]*POST_COMMENT( \\w+)? WHERE [\\w_$]*.ID=\\?");    }

    @Test
    void givenManyToOneUnidirectionalRelationshipLazyFetch_whenGetOneSideReferenceFromManySideEntity_thenOneSideIsFetchedByIdInSeparatedQuery() {

        Long createdFirstCommentId = transactionTemplate.execute(status -> {

            Post post = new Post("Some post");

            PostComment firstComment = new PostComment("First comment");
            firstComment.setPost(post);

            PostComment secondComment = new PostComment("Second comment");
            secondComment.setPost(post);

            entityManager.persist(post);
            entityManager.persist(firstComment);
            entityManager.persist(secondComment);

            return firstComment.getId();
        });

        ptds.reset();  // reset query execution logging

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            PostComment foundFirstComment = entityManager.find(PostComment.class, createdFirstCommentId);
            foundFirstComment.getPost().toString();
        });

        // then
        assertThat(ptds).hasSelectCount(2);
        List<String> selectQueryList = ptds.getPrepareds().stream()
                .map(PreparedExecution::getQuery)
                .map(String::toUpperCase)
                .filter(query -> query.startsWith("SELECT"))
                .collect(Collectors.toList());
        assertThat(selectQueryList).hasSize(2);
        assertThat(selectQueryList.get(0)).matches("SELECT .+ FROM [\\w_$]*POST_COMMENT( \\w+)? WHERE [\\w_$]*.ID=\\?");
        assertThat(selectQueryList.get(1)).matches("SELECT .+ FROM [\\w_$]*POST( \\w+)? WHERE [\\w_$]*.ID=\\?");
    }

    @Test
    void givenManyToOneUnidirectionalRelationship_theManySideCollectionIsNotManagedAnymore_theManySideCollectionOfSpecificOneSideEntityCanBeFoundByOneSideId() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(status -> {

            Post post = new Post("Some post");
            entityManager.persist(post);

            PostComment firstComment = new PostComment("First comment");
            firstComment.setPost(post);
            entityManager.persist(firstComment);

            PostComment secondComment = new PostComment("Second comment");
            secondComment.setPost(post);
            entityManager.persist(secondComment);

            entityManager.flush();

            return post.getId();
        });

        ptds.reset();  // reset query execution logging

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            // when
            List<PostComment> postCommentList = entityManager.createQuery(
                    "SELECT pc " +
                            "FROM " + PostComment.class.getName() + " pc " +
                            "WHERE pc.post.id = :postId",
                    PostComment.class)
                    .setParameter("postId", createdPostId)
                    .getResultList();

            // then
            assertThat(ptds).hasSelectCount(1);
            List<String> selectQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("SELECT"))
                    .collect(Collectors.toList());
            assertThat(selectQueryList).hasSize(1);
            assertThat(selectQueryList.get(0)).matches(
                    "SELECT .+ FROM [\\w_$]*POST_COMMENT( \\w+)? WHERE ([\\w_$]+.)?POST_ID=\\?");
        });
    }

    @Test
    void givenManyToOneUnidirectionalRelationship_theManySideCollectionIsNotManagedAnymore_theManySideCollectionOfSpecificOneSideEntityCanBeFoundByOneSideReference() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(status -> {

            Post post = new Post("Some post");
            entityManager.persist(post);

            PostComment firstComment = new PostComment("First comment");
            firstComment.setPost(post);
            entityManager.persist(firstComment);

            PostComment secondComment = new PostComment("Second comment");
            secondComment.setPost(post);
            entityManager.persist(secondComment);

            entityManager.flush();

            return post.getId();
        });

        ptds.reset();  // reset query execution logging

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);

            ptds.reset();  // reset query execution logging

            // when
            List<PostComment> postCommentList = entityManager.createQuery(
                    "SELECT pc " +
                            "FROM " + PostComment.class.getName() + " pc " +
                            "WHERE pc.post = :post",
                    PostComment.class)
                    .setParameter("post", foundPost)
                    .getResultList();

            // then
            assertThat(ptds).hasSelectCount(1);
            List<String> selectQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("SELECT"))
                    .collect(Collectors.toList());
            assertThat(selectQueryList).hasSize(1);
            assertThat(selectQueryList.get(0)).matches(
                    "SELECT .+ FROM [\\w_$]*POST_COMMENT( \\w+)? WHERE ([\\w_$]+.)?POST_ID=\\?");
        });
    }

    @Test
    void givenManyToOneUnidirectionalRelationship_whenRemoveManySideEntityFromCollection_thenOnlyOneDeleteStatementGetExecuted() {

        transactionTemplate.executeWithoutResult(status -> {

            // data preparation
            Post post = new Post("Some post");
            entityManager.persist(post);

            PostComment firstComment = new PostComment("First comment");
            firstComment.setPost(post);
            entityManager.persist(firstComment);

            PostComment secondComment = new PostComment("Second comment");
            secondComment.setPost(post);
            entityManager.persist(secondComment);

            entityManager.flush();

            ptds.reset();  // reset query execution logging

            // when
            entityManager.remove(firstComment);
            entityManager.flush();

            // then
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
