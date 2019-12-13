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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.persistence.*;

import static net.ttddyy.dsproxy.asserts.assertj.DataSourceAssertAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(properties = {
        "spring.jpa.properties.hibernate.generate_statistics=true"})
@Import(ProxyTestDataSourceConfig.class)
class ManyToManyBidirectionalListTest {

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

        @ManyToMany(cascade = {CascadeType.PERSIST, CascadeType.MERGE})
        @JoinTable(name = "many_to_many_bidirectional_list_test$post_tag",
                joinColumns = @JoinColumn(name = "post_id"),
                inverseJoinColumns = @JoinColumn(name = "tag_id"))
        private List<Tag> tags = new ArrayList<>();

        Post() {}

        Post(String title) { this.title = title; }

        void addTag(Tag tag) {

            tags.add(tag);
            tag.getPosts().add(this);
        }

        void removeTag(Tag tag) {

            tags.remove(tag);
            tag.getPosts().remove(this);
        }
    }

    @Entity
    @Data
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class Tag {

        @Id
        @GeneratedValue
        private Long id;

        @EqualsAndHashCode.Include
        private String name;

        @ManyToMany(mappedBy = "tags")
        private List<Post> posts = new ArrayList<>();

        Tag() {}

        Tag(String name) { this.name = name; }
    }

    /**
     * When remove a {@code Tag} entity from a {@code Post}, Hibernate removes all {@code post_tag} rows associated with
     * the given {@code post_id} and reinserts the remaining ones back afterward.
     */
    @Test
    void givenBidirectionalManyToManyList_whenRemoveElemFromList_thenFirstDeleteJoinTableRowAssociatedToTheOwnerIdAndThenReinsertTheRemainingBack() {

        Long jpaHibernatePostId = transactionTemplate.execute(transactionStatus -> {

            Post jpaHibernatePost = new Post("JPA with Hibernate");
            Post hibernatePost = new Post("Native Hibernate");

            Tag jpaTag = new Tag("JPA");
            Tag hibernateTag = new Tag("Hibernate");

            jpaHibernatePost.addTag(jpaTag);
            jpaHibernatePost.addTag(hibernateTag);

            hibernatePost.addTag(hibernateTag);

            entityManager.persist(jpaHibernatePost);
            entityManager.persist(hibernatePost);

            assertNotNull(jpaHibernatePost.getId());
            assertNotNull(hibernatePost.getId());
            assertNotNull(jpaTag.getId());
            assertNotNull(hibernateTag.getId());

            return jpaHibernatePost.getId();
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            Post foundJpaHibernatePost = entityManager.find(Post.class, jpaHibernatePostId);
            Optional<Tag> jpaTag = foundJpaHibernatePost.getTags().stream().filter(tag -> tag.getName().equals("JPA")).findFirst();
            assertThat(jpaTag).isPresent();

            ptds.reset();  // reset query execution logging

            // when
            foundJpaHibernatePost.removeTag(jpaTag.get());
            entityManager.flush();

            // then
            assertThat(ptds).hasDeleteCount(1);
            assertThat(ptds).hasInsertCount(1);
            List<String> deleteOrInsertQueryList = ptds.getPrepareds().stream()
                    .map(PreparedExecution::getQuery)
                    .map(String::toUpperCase)
                    .filter(query -> query.startsWith("DELETE") || query.startsWith("INSERT"))
                    .collect(Collectors.toList());
            assertThat(deleteOrInsertQueryList).hasSize(2);
            assertThat(deleteOrInsertQueryList.get(0)).matches("DELETE FROM [\\w_$]*POST_TAG( \\w+)? WHERE POST_ID=\\?");
            assertThat(deleteOrInsertQueryList.get(1)).matches("INSERT INTO [\\w_$]*POST_TAG .+");
        });
    }
}
