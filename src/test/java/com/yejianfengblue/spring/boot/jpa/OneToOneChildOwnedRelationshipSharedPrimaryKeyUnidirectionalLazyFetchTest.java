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

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import javax.persistence.*;

import static net.ttddyy.dsproxy.asserts.assertj.DataSourceAssertAssertions.assertThat;

/**
 * @author yejianfengblue
 */
@SpringBootTest(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true"})
@Import(ProxyTestDataSourceConfig.class)
class OneToOneChildOwnedRelationshipSharedPrimaryKeyUnidirectionalLazyFetchTest {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private ProxyTestDataSource ptds;

    private Logger log = LoggerFactory.getLogger(getClass());

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

        /*
        Don't even need a bidirectional association since can always fetch
        the PostDetail entity by using the Post entity ID
         */
    }

    @Entity
    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class PostDetail {

        @Id
        private Long id;

        @EqualsAndHashCode.Include
        @ToString.Exclude
        @OneToOne(fetch = FetchType.LAZY, optional = false)
        @MapsId
        // @JoinColumn can customize the primary key column
        private Post post;

        private Instant createdTime;

        private String createdBy;
    }

    @Test
    void givenChildOwnedOneToOneUnidirectionalRelationshipWithSharedPkAndChildSideLazyFetch_whenFindChildWithParentId_thenChildIsFetchedSuccessfully() {

        // data preparation
        Long createdPostId = transactionTemplate.execute(transactionStatus -> {

            Post newPost = new Post();
            newPost.setTitle("Dummy Title");
            entityManager.persist(newPost);

            PostDetail newPostDetail = new PostDetail();
            newPostDetail.setCreatedBy("x");
            newPostDetail.setCreatedTime(Instant.now());
            newPostDetail.setPost(newPost);
            entityManager.persist(newPostDetail);

            return newPost.getId();
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            // when
            Post foundPost = entityManager.find(Post.class, createdPostId);
            PostDetail foundPostDetail = entityManager.find(PostDetail.class, foundPost.getId());

            // then
            assertThat(ptds).hasSelectCount(2);
            List<String> selectList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("SELECT"))
                    .collect(Collectors.toList());
            Assertions.assertThat(selectList).hasSize(2);
            Assertions.assertThat(selectList.get(0)).matches("SELECT .+ FROM [\\w_$]*POST( \\w+)? WHERE .+");
            Assertions.assertThat(selectList.get(1)).matches("SELECT .+ FROM [\\w_$]*POST_DETAIL( \\w+)? WHERE .+");
        });
    }
}
