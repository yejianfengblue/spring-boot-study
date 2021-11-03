package com.yejianfengblue.spring.boot.tx;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import net.ttddyy.dsproxy.asserts.ProxyTestDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.support.TransactionTemplate;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EntityScan(basePackageClasses = OneToManyUseParentVersionTest.class)
@Import(ProxyTestDataSourceConfig.class)
class OneToManyUseParentVersionTest {

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private ProxyTestDataSource ptds;

    private Logger log = LoggerFactory.getLogger(getClass());

    @Entity
    @Data
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class Post {

        @Id
        @GeneratedValue
        private Long id;

        @Version
        private Integer version;

        @EqualsAndHashCode.Include
        private String title;

        @OneToMany(mappedBy = "post", cascade = CascadeType.ALL,fetch = FetchType.EAGER, orphanRemoval = true)
        @ToString.Exclude
        private List<PostComment> postCommentList = new ArrayList<>();

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
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class PostComment {

        @Id
        @GeneratedValue
        private Long id;

        @EqualsAndHashCode.Include
        private String review;

        @ManyToOne(fetch = FetchType.EAGER)
        @JoinColumn(name = "post_id")
        private Post post;

        PostComment(String review) {
            this.review = review;
        }
    }

    /**
     * Reset query execution logging
     */
    @BeforeEach
    void resetProxyTestDataSource() {

        ptds.reset();
    }

    @DisplayName("Given parent version, when update parent, then parent version get incremented")
    @Test
    void givenParentVersion_whenUpdateParent_thenParentVersionGetIncremented() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(status -> {

            Post post = new Post("Hail Hydra");
            post.addPostComment(new PostComment("Hail Hydra"));
            post.addPostComment(new PostComment("Immortal Hydra"));

            entityManager.persist(post);
            entityManager.flush();

            return post.getId();
        });

        transactionTemplate.executeWithoutResult(status -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);
            assertEquals(0, foundPost.getVersion());

            foundPost.setTitle("abc");
            entityManager.persist(foundPost);
            entityManager.flush();
        });

        assertEquals(1, entityManager.find(Post.class, createdPostId).getVersion());
    }

    @DisplayName("Given parent version, when update child, then parent version not incremented")
    @Test
    void givenParentVersion_whenUpdateChild_thenParentVersionNotIncremented() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(status -> {

            Post post = new Post("Hail Hydra");
            post.addPostComment(new PostComment("Hail Hydra"));
            post.addPostComment(new PostComment("Immortal Hydra"));

            entityManager.persist(post);
            entityManager.flush();

            return post.getId();
        });

        transactionTemplate.executeWithoutResult(status -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);
            assertEquals(0, foundPost.getVersion());

            foundPost.getPostCommentList().get(0).setReview("Superman");
            entityManager.persist(foundPost);
            entityManager.flush();
        });

        assertEquals(0, entityManager.find(Post.class, createdPostId).getVersion());
    }

    @DisplayName("Given parent version, when update child collection, then parent version not incremented")
    @Test
    void givenParentVersion_whenUpdateChildCollection_thenParentVersionNotIncremented() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(status -> {

            Post post = new Post("Hail Hydra");
            post.addPostComment(new PostComment("Hail Hydra"));
            post.addPostComment(new PostComment("Immortal Hydra"));

            entityManager.persist(post);
            entityManager.flush();

            return post.getId();
        });

        transactionTemplate.executeWithoutResult(status -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);
            assertEquals(0, foundPost.getVersion());

            foundPost.getPostCommentList().clear();
            entityManager.persist(foundPost);
            entityManager.flush();
        });

        assertEquals(0, entityManager.find(Post.class, createdPostId).getVersion());
    }


}
