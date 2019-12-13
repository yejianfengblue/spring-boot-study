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
class OneToOneChildOwnedRelationshipOrphanRemovalTrueTest {

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

        @OneToOne(mappedBy = "post",
                cascade = CascadeType.ALL,
                fetch = FetchType.LAZY,
                orphanRemoval = true)
        @ToString.Exclude
        private PostDetail postDetail;
    }

    @Entity
    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class PostDetail {

        @Id
        @GeneratedValue
        private Long id;

        private Boolean visible;

        @EqualsAndHashCode.Include
        @ToString.Exclude
        @OneToOne(fetch = FetchType.LAZY, optional = true)
        @JoinColumn(name = "post_id")
        private Post post;
    }

    @Test
    void givenChildOwnedOneToOneRelationshipWithOrphanRemovalTrue_whenParentSideChildFieldToNull_thenChildIsAutoDeleted() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(transactionStatus -> {

            Post newPost = new Post();
            newPost.setTitle("Dummy Title");
            PostDetail newPostDetail = new PostDetail();
            newPostDetail.setVisible(true);
            newPost.setPostDetail(newPostDetail);
            newPostDetail.setPost(newPost);

            entityManager.persist(newPost);
            return newPost.getId();
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);
            PostDetail foundPostDetail = foundPost.getPostDetail();

            ptds.reset();  // reset query execution logging

            // when
            foundPost.setPostDetail(null);  // better use Post.removePostDetail combo method to set PostDetail.post to null also
            entityManager.flush();

            // then
            assertThat(ptds).hasDeleteCount(1);
            List<String> deleteQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("DELETE"))
                    .collect(Collectors.toList());
            assertThat(deleteQueryList).hasSize(1);
            assertThat(deleteQueryList.get(0)).matches("DELETE FROM [\\w_$]*POST_DETAIL( \\w+)? WHERE ID=\\?");
        });
    }

    @Test
    void givenChildOwnedOneToOneRelationshipWithOrphanRemovalTrue_whenChildSideParentFieldToNull_thenChildIsAutoDeletedButUpdatedFkToNull() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(transactionStatus -> {

            Post newPost = new Post();
            newPost.setTitle("Dummy Title");
            PostDetail newPostDetail = new PostDetail();
            newPostDetail.setVisible(true);
            newPost.setPostDetail(newPostDetail);
            newPostDetail.setPost(newPost);

            entityManager.persist(newPost);
            return newPost.getId();
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);
            PostDetail foundPostDetail = foundPost.getPostDetail();

            ptds.reset();  // reset query execution logging

            // when
            foundPostDetail.setPost(null);
            entityManager.flush();

            // then
            assertThat(ptds).hasDeleteCount(0);
            assertThat(ptds).hasUpdateCount(1);
            List<String> updateQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("UPDATE"))
                    .collect(Collectors.toList());
            assertThat(updateQueryList).hasSize(1);
            assertThat(updateQueryList.get(0)).matches("UPDATE [\\w_$]*POST_DETAIL( \\w+)? .+");
        });
    }

}
