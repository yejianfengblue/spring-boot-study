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

import java.util.ArrayList;
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
class OneToManyParentOwnedUnidirectionalRelationshipEagerFetchTest {

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

        @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, orphanRemoval = true)
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
    void givenEagerFetchUninitializedCollection_whenUseThatCollectionWithinSameTransaction_thenCollectionIsFetchedAlongInSingleQuery() {

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
                            "LEFT OUTER JOIN [\\w_$]*POST_COMMENT( [\\w_]+)? " +
                            "ON [\\w_.=]+ " +
                            "WHERE .+");
        });
    }
}
