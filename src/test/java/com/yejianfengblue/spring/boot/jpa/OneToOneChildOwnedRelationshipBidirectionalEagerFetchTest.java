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

import java.time.Instant;
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
class OneToOneChildOwnedRelationshipBidirectionalEagerFetchTest {

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
                fetch = FetchType.EAGER,
                optional = false)
        @ToString.Exclude
        private PostDetail postDetail;

        void setDetail(PostDetail postDetail) {

            if (null == postDetail) {
                if (null != this.postDetail) {
                    this.postDetail.setPost(null);
                }
            } else {
                postDetail.setPost(this);
            }
            this.postDetail = postDetail;
        }
    }

    @Entity
    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class PostDetail {

        @Id
        @GeneratedValue
        private Long id;

        @EqualsAndHashCode.Include
        @ToString.Exclude
        @OneToOne(fetch = FetchType.EAGER, optional = false)
        @JoinColumn(name = "post_id")
        private Post post;

        private Instant createdTime;

        private String createdBy;
    }

    @Test
    void givenChildOwnedOneToOneRelationshipAndParentSideEagerFetch_whenFindParent_thenChildIsFetchedAlongInSingleQuery() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(transactionStatus -> {

            Post newPost = new Post();
            newPost.setTitle("Dummy Title");
            PostDetail newPostDetail = new PostDetail();
            newPostDetail.setCreatedBy("x");
            newPostDetail.setCreatedTime(Instant.now());
            newPost.setDetail(newPostDetail);

            entityManager.persist(newPost);
            return newPost.getId();
        });

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
                        "INNER JOIN [\\w_$]*POST_DETAIL( [\\w_]+)? " +
                            "ON [\\w_.=]+ " +
                    "WHERE .+");
        });
    }

    @Test
    void givenChildOwnedOneToOneRelationshipAndChildSideEagerFetch_whenFindChild_thenParentIsFetchedAlongInSingleQuery() {

        // data preparation
        Long createdPostDetailId = transactionTemplate.execute(transactionStatus -> {

            Post newPost = new Post();
            newPost.setTitle("Dummy Title");
            PostDetail newPostDetail = new PostDetail();
            newPostDetail.setCreatedBy("x");
            newPostDetail.setCreatedTime(Instant.now());
            newPost.setDetail(newPostDetail);

            entityManager.persist(newPost);
            return newPostDetail.getId();
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            // when
            PostDetail foundPostDetail = entityManager.find(PostDetail.class, createdPostDetailId);

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
                            "FROM [\\w_$]*POST_DETAIL( [\\w_]+)? " +
                            "INNER JOIN [\\w_$]*POST( [\\w_]+)? " +
                            "ON [\\w_.=]+ " +
                            "WHERE .+");
        });
    }
}
