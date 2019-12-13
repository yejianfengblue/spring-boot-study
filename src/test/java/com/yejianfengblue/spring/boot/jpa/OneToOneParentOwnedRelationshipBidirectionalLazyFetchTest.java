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

import java.time.Instant;
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
class OneToOneParentOwnedRelationshipBidirectionalLazyFetchTest {

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

        @OneToOne(
                cascade = CascadeType.ALL,
                fetch = FetchType.LAZY,
                optional = false)
        @JoinColumn(name = "post_detail_id")
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

        @OneToOne(mappedBy = "postDetail", fetch = FetchType.LAZY)
        private Post post;

        private Instant createdTime;

        private String createdBy;
    }

    @Test
    void givenParentOwnOneToOneRelationshipAndParentSideLazyFetch_whenFindParent_thenChildIsNotAutoFetched() {

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
            List<String> selectList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("SELECT"))
                    .collect(Collectors.toList());
            assertThat(selectList).hasSize(1);
            assertThat(selectList.get(0)).matches("SELECT .+ FROM [\\w_$]*POST( \\w+)? WHERE .+");
        });
    }

    @Test
    void givenParentOwnOneToOneRelationshipAndChildSideLazyFetch_whenFindChild_thenParentIsAutoFetchedInSecondaryQuery() {

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
            assertThat(ptds).hasSelectCount(2);
            List<String> selectQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("SELECT"))
                    .collect(Collectors.toList());
            assertThat(selectQueryList).hasSize(2);
            assertThat(selectQueryList.get(0)).matches("SELECT .+ FROM [\\w_$]*POST_DETAIL( \\w+)? WHERE .+");
            assertThat(selectQueryList.get(1)).matches("SELECT .+ FROM [\\w_$]*POST( \\w+)? WHERE .+");
        });
    }

    @Test
    void givenLazyFetchUninitializedHibernateProxy_whenUseThatProxyOutOfOriginTransaction_thenLazyInitializationException() {

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

        // given
        PostDetail proxyPostDetail = transactionTemplate.execute(transactionStatus -> {

            Post foundPost = entityManager.find(Post.class, createdPostId);
            assertThat(foundPost).isNotNull();
            return foundPost.getPostDetail();
        });

        // then
        assertThatThrownBy(() -> {
            // when
            proxyPostDetail.toString();
        }).isInstanceOf(LazyInitializationException.class)
                .hasMessageContaining("no Session");
    }

    @Test
    void givenLazyFetchUninitializedHibernateProxy_whenUseThatProxyWithinSameTransaction_thenProxyTargetIsFetchedInSeparateQuery() {

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

            Post foundPost = entityManager.find(Post.class, createdPostId);
            ptds.reset();  // reset query execution logging

            // given
            PostDetail proxyPostDetail = foundPost.getPostDetail();  // here get the hibernate proxy

            // when
            proxyPostDetail.toString();  // this trigger select from post_detail

            // then
            assertThat(ptds).hasSelectCount(1);
            List<String> selectQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("SELECT"))
                    .collect(Collectors.toList());
            assertThat(selectQueryList).hasSize(1);
            assertThat(selectQueryList.get(0)).matches("SELECT .+ FROM [\\w_$]*POST_DETAIL( \\w+)? WHERE .+");
        });
    }

}
