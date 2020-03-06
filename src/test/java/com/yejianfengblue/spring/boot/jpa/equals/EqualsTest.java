package com.yejianfengblue.spring.boot.jpa.equals;

import com.yejianfengblue.spring.boot.jpa.ProxyTestDataSourceConfig;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.LazyInitializationException;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.support.TransactionTemplate;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A test class to demo the best way to implement equals() and hashCode() of JPA entity
 * <p>
 * For root entity (an entity without any parent dependency):
 * <ol>
 *     <li>Use business key composed with column(s) declared unique, non-nullable and non-updatable</li>
 *     <li>If use default Object equals() and hashCode(), then entity proxy is not equal with entity</li>
 *     <li>If no business key:
 *     <p>
 *         <ol>
 *             <li>If use entity ID, then hashCode differs between new (transient) and after persisted</li>
 *             <li>If use entity ID for equals() and hashCode() always returns same value, then test passes but it defeats
 *         the purpose of using multiple buckets in a HashSet or HashMap. Use list instead.</li>
 *         </ol>
 *     </p>
 * </ol>
 * </p>
 * <p>In a unidirectional @ManyToOne relationship, for child entity {@link Product} with
 * an <b>EAGER</b> fetched parent {@link Company}, the child entity business key
 * includes {@code code} and parent entity.</p>
 * <p>In a unidirectional @ManyToOne relationship, for child entity {@link Image} with
 * an <b>LAZY</b> fetched parent {@link Product}, the child entity business key
 * mustn't include parent entity. Otherwise, if call hashCode with unitialized parent and persistence context is closed,
 * then LazyInitializationException</p>
 * <p>In a bidirectional @ManyToOne relationship, for child entity {@link MonsterCardEffect} with
 * an <b>EAGER</b> fetched parent {@link MonsterCard}, the child entity business key
 * includes {@code index} and parent entity. If the child collection at parent side is a HashSet,
 * the two utility methods addEffect() and removeEffect() at parent entity side must handle carefully
 * the order calling add(effect), setCard(card), remove(effect), setCard(null), so as the hashCode is aligned.</p>
 *
 *  @see <a href="https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/">
 *      How to implement equals and hashCode using the JPA entity identifier (Primary Key)</a>
 *  @see <a href="https://vladmihalcea.com/hibernate-facts-equals-and-hashcode/">
 *      How to implement Equals and HashCode for JPA entities</a>
 *  @see <a href="https://vladmihalcea.com/the-best-way-to-implement-equals-hashcode-and-tostring-with-jpa-and-hibernate/">
 *      The best way to implement equals, hashCode, and toString with JPA and Hibernate</a>
 */
@SpringBootTest
@EntityScan(basePackageClasses = EqualsTest.class)
@Slf4j
@Import(ProxyTestDataSourceConfig.class)
class EqualsTest {

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private EntityManager entityManager;

    protected static interface EntityInterface {

        public Long getId();
    }

    protected <T extends EntityInterface> void assertEqualityConsistency(Class<T> clazz, T entity) {

        HashSet<T> entityHashSet = new HashSet<>();

        assertFalse(entityHashSet.contains(entity));
        entityHashSet.add(entity);
        assertTrue(entityHashSet.contains(entity));

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            entityManager.persist(entity);
            entityManager.flush();
            assertTrue(entityHashSet.contains(entity),
                    "The entity is not found in the Set after it's persisted");
        });

        assertTrue(entityHashSet.contains(entity));

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            T entityProxy = entityManager.getReference(clazz, entity.getId());
            assertTrue(entityProxy.equals(entity),
                    "The entity proxy is not equal with the entity");
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            T entityProxy = entityManager.getReference(clazz, entity.getId());
            assertTrue(entity.equals(entityProxy),
                    "The entity is not equal with the entity proxy");
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            T _entity = entityManager.merge(entity);
            assertTrue(entityHashSet.contains(_entity),
                    "The entity is not found in the Set after it's merged");
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            entityManager.unwrap(Session.class).update(entity);
            assertTrue(entityHashSet.contains(entity),
                    "The entity is not found in the Set after it's reattached");
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            T _entity = entityManager.find(clazz, entity.getId());
            assertTrue(entityHashSet.contains(_entity),
                    "The entity is not found in the Set after it's loaded in a subsequent Persistence Context");
        });

        transactionTemplate.executeWithoutResult(transactionStatus -> {

            T _entity = entityManager.getReference(clazz, entity.getId());
            assertTrue(entityHashSet.contains(_entity),
                    "The entity is not in the Set found after it's loaded as a proxy in other Persistence Context");
        });

        T deletedEntity = transactionTemplate.execute(transactionStatus -> {

            T _entity = entityManager.getReference(clazz, entity.getId());
            entityManager.remove(_entity);
            return _entity;
        });

        assertTrue(entityHashSet.contains(deletedEntity),
                "The entity is found in not the Set even after it's deleted");
    }

    @Entity
    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    private static class BookBusinessKey implements EntityInterface {

        @Id
        @GeneratedValue
        @Setter(AccessLevel.NONE)
        private Long id;

        @Column(unique = true, nullable = false, updatable = false)
        private String isbn;

        private String title;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BookBusinessKey)) return false;
            BookBusinessKey book = (BookBusinessKey) o;
            return Objects.equals(getIsbn(), book.getIsbn());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getIsbn());
        }
    }

    @Test
    void givenEntityImplEqualsAndHashCodeWithBusinessKey_whenTestEqualityConsistency_thenPass() {

        BookBusinessKey book = new BookBusinessKey();
        book.setIsbn("123-456-7890");
        book.setTitle("Some Title");

        assertEqualityConsistency(BookBusinessKey.class, book);
    }

    @Entity
    @Getter
    @Setter
    @NoArgsConstructor
    private static class BookDefaultObjectEqualsAndHashCode implements EntityInterface {

        @Id
        @GeneratedValue
        @Setter(AccessLevel.NONE)
        private Long id;

        private String title;
    }

    @Test
    void givenEntityUsingDefaultObjectEqualsAndHashCode_whenTestEqualityConsistency_thenErrorTheEntityProxyIsNotEqualWithTheEntity() {

        BookDefaultObjectEqualsAndHashCode book = new BookDefaultObjectEqualsAndHashCode();
        book.setTitle("Some title");

        assertEqualityConsistency(BookDefaultObjectEqualsAndHashCode.class, book);
        // throws "The entity proxy is not equal with the entity"
        // the original entity is not equals with the returned "reference"
    }

    @Entity
    @Getter
    @Setter
    @NoArgsConstructor
    private static class BookEntityId implements EntityInterface {

        @Id
        @GeneratedValue
        @Setter(AccessLevel.NONE)
        private Long id;

        private String title;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BookEntityId)) return false;
            BookEntityId book = (BookEntityId) o;
            return Objects.equals(getId(), book.getId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getId());
        }
    }

    @Test
    void givenEntityImplEqualsAndHashCodeWithEntityId_whenTestEqualityConsistency_thenErrorTheEntityIsNotFoundInTheSetAfterPersisted() {

        BookEntityId book = new BookEntityId();
        book.setTitle("Some title");

        assertEqualityConsistency(BookEntityId.class, book);
        // The entity is not found in the Set after it's persisted
        /* When the entity was first stored in the Set, the identifier was null.
         * After the entity was persisted, the identifier was assigned to an auto-generated value,
         * hence the hashCode differs.
         */
    }

    // To fix the entity identifier equals and hashcode, there is only one solution:
    // the hashcode should always return the same value

    @Entity
    @Getter
    @NoArgsConstructor
    private static class BookEntityIdAndHashCodeAlwaysReturnSameValue implements EntityInterface {

        @Id
        @GeneratedValue
        private Long id;

        @Setter
        private String title;

        @Override
        public boolean equals(Object o) {

            if (this == o)
                return true;
            if (!(o instanceof BookEntityIdAndHashCodeAlwaysReturnSameValue))
                return false;
            BookEntityIdAndHashCodeAlwaysReturnSameValue other = (BookEntityIdAndHashCodeAlwaysReturnSameValue) o;

            return null != id && id.equals(other.getId());  // can't use id.equals(other.id) because other might be a proxy
        }

        @Override
        public int hashCode() {
            return 31;
        }
    }

    @Test
    void givenEntityImplEqualsWithEntityIdAndHashCodeAlwaysReturnSameValue_whenTestEqualityConsistency_then() {

        BookEntityIdAndHashCodeAlwaysReturnSameValue book = new BookEntityIdAndHashCodeAlwaysReturnSameValue();
        book.setTitle("Some title");

        assertEqualityConsistency(BookEntityIdAndHashCodeAlwaysReturnSameValue.class, book);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Unidirectional many-to-one relationship, child entity with an EAGER fetched parent

    @Entity
    @Getter
    @Setter
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class Company implements EntityInterface {

        @Id
        @GeneratedValue
        private Long id;

        @Column(unique = true, nullable = false, updatable = false)
        @EqualsAndHashCode.Include
        private String name;
    }

    @Entity
    @Getter
    @Setter
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class Product implements EntityInterface {

        @Id
        @GeneratedValue
        @Setter(AccessLevel.NONE)
        private Long id;

        @Column(unique = true, nullable = false, updatable = false)
        @EqualsAndHashCode.Include
        private String code;

        @ManyToOne(fetch = FetchType.EAGER, optional = false)
        @JoinColumn(name = "company_id", nullable = false, updatable = false)
        @EqualsAndHashCode.Include
        private Company company;
    }

    // Unidirectional many-to-one relationship, child entity with a LAZY fetched parent
    @Entity
    @Getter
    @Setter
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class Image implements EntityInterface {

        @Id
        @GeneratedValue
        @Setter(AccessLevel.NONE)
        private Long id;

        @Column(updatable = false)
        @EqualsAndHashCode.Include
        private String name;

        @ManyToOne(fetch = FetchType.LAZY, optional = false)
        @JoinColumn(name = "produce_id", nullable = false, updatable = false)
        private Product product;
    }

    @Entity
    @Getter
    @Setter
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class ImageEqualsAndHashCodeIncludeProduct implements EntityInterface {

        @Id
        @GeneratedValue
        @Setter(AccessLevel.NONE)
        private Long id;

        @Column(updatable = false)
        @EqualsAndHashCode.Include
        private String name;

        @ManyToOne(fetch = FetchType.LAZY, optional = false)
        @JoinColumn(name = "produce_id", nullable = false, updatable = false)
        @EqualsAndHashCode.Include
        private Product product;
    }

    @Test
    void givenChildEntityWithEagerFetchedParent_whenTestEqualityConsistency_thenPass() {

        Company ibm = new Company();
        ibm.setName("IBM");

        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(ibm);
        });

        Product ibm5100 = new Product();
        ibm5100.setCompany(ibm);
        ibm5100.setCode("ibm5100");
        assertEqualityConsistency(Product.class, ibm5100);
    }

    @Test
    void givenChildEntityWithLazyFetchedParent_whenTestEqualityConsistency_thenPass() {

        Company ibm = new Company();
        ibm.setName("IBM_2");

        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(ibm);
        });

        Product ibm5100 = new Product();
        ibm5100.setCompany(ibm);
        ibm5100.setCode("ibm5100_2");
        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(ibm5100);
        });

        Image frontImage = new Image();
        frontImage.setName("front image 2");
        frontImage.setProduct(ibm5100);

        assertEqualityConsistency(Image.class, frontImage);
    }

    @Test
    void givenChildEntityWithLazyFetchedParent_whenCallChildHashCodeWithUninitializedParentOutOfPersistenceContext_thenOk() {

        Company ibm = new Company();
        ibm.setName("IBM_3");

        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(ibm);
        });

        Product ibm5100 = new Product();
        ibm5100.setCompany(ibm);
        ibm5100.setCode("ibm5100_3");
        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(ibm5100);
        });

        Image frontImage = new Image();
        frontImage.setName("front image 3");
        frontImage.setProduct(ibm5100);
        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(frontImage);
        });

        Image detachedFrontImage = transactionTemplate.execute(transactionStatus -> {
            return entityManager.find(Image.class, frontImage.getId());
        });
        assertDoesNotThrow(() -> detachedFrontImage.hashCode());
    }

    @Test
    void givenChildEntityWithLazyFetchedParent_whenCallChildHashCodeWithUninitializedProductOutOfPersistenceContext_thenLazyInitializationException() {

        Company ibm = new Company();
        ibm.setName("IBM_4");

        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(ibm);
        });

        Product ibm5100 = new Product();
        ibm5100.setCompany(ibm);
        ibm5100.setCode("ibm5100_4");
        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(ibm5100);
        });

        ImageEqualsAndHashCodeIncludeProduct frontImage = new ImageEqualsAndHashCodeIncludeProduct();
        frontImage.setName("front image 4");
        frontImage.setProduct(ibm5100);
        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(frontImage);
        });

        ImageEqualsAndHashCodeIncludeProduct detachedFrontImage = transactionTemplate.execute(transactionStatus -> {
            return entityManager.find(ImageEqualsAndHashCodeIncludeProduct.class, frontImage.getId());
        });
        assertThrows(LazyInitializationException.class, () -> detachedFrontImage.hashCode());
    }


    ///////////////////////////////////////////////////////////////////////////
    /* bidirectional one-to-many relationship, parent manages the child collection in a HashSet and has two utility
     * methods addChild() and removeChild().
     * child entity with an EAGER parent, the EqualsAndHashCode includes parent and unique column.
     */
    @Entity
    @Getter
    @Setter
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class MonsterCard {

        @Id
        @GeneratedValue
        private Long id;

        @Column(unique = true, nullable = false, updatable = false)
        @EqualsAndHashCode.Include
        private String code;

        @Column(nullable = false, updatable = false)
        private String name;

        @OneToMany(mappedBy = "card",
                cascade = CascadeType.ALL,
                fetch = FetchType.LAZY,
                orphanRemoval = true)
        private Set<MonsterCardEffect> effects = new HashSet<>();

        // the effects HashSet use the effect hash version where card is set (not null)

        void addEffect(MonsterCardEffect effect) {

            // because MonsterCard is included in MonsterCardEffect's EqualsAndHashCode
            // set card must be before add into set, so the set hash is same as the hashCode() value including card
            effect.setCard(this);
            effects.add(effect);
        }

        void removeEffect(MonsterCardEffect effect) {

            // because MonsterCard is included in MonsterCardEffect's EqualsAndHashCode
            // remove from set must be before set card to null
            effects.remove(effect);
            effect.setCard(null);
        }

        void removeEffect_SetParentNullBeforeRemoveFromHashSet(MonsterCardEffect effect) {

            effect.setCard(null);
            // after set card to null, the effect.hashCode() changes, and below remove() may not remove in fact
            effects.remove(effect);
        }
    }

    @Entity
    @Getter
    @Setter
    @NoArgsConstructor
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    private static class MonsterCardEffect implements EntityInterface{

        @Id
        @GeneratedValue
        private Long id;

        @Column(nullable = false, updatable = false)
        @EqualsAndHashCode.Include
        private Integer index;

        private String description;

        @ManyToOne(fetch = FetchType.EAGER, optional = false)
        @JoinColumn(name = "card_id", nullable = false, updatable = false)
        @EqualsAndHashCode.Include
        private MonsterCard card;
    }

    @Test
    void givenBidirectionalOneToManyRelationship_andParentManagesChildCollectionInHashSet_andParentUtilMethodsAlignChildHashCorrectly_andChildEqualsAndHashCodeIncludeParent___whenCallUtilMethodRemove___thenRemoveSuccessfully() {

        MonsterCard yuJyo = new MonsterCard();
        yuJyo.setCode("81332143");
        yuJyo.setName("YU-JYO");

        MonsterCardEffect shakeHand = new MonsterCardEffect();
        shakeHand.setIndex(1);
        shakeHand.setDescription("Send a hand shake invitation to opponent");

        log.info("shakeHand.hashCode before add = {}", shakeHand.hashCode());
        yuJyo.addEffect(shakeHand);
        log.info("shakeHand.hashCode after add = {}", shakeHand.hashCode());

        yuJyo.getEffects().contains(shakeHand);

        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(yuJyo);
            entityManager.flush();

            assertNotNull(yuJyo.getId());
            assertNotNull(shakeHand.getId());
            assertEquals(1, yuJyo.getEffects().size());

            log.info("shakeHand.hashCode before remove = {}", shakeHand.hashCode());
            yuJyo.removeEffect(shakeHand);
            log.info("shakeHand.hashCode after remove = {}", shakeHand.hashCode());

            assertEquals(0, yuJyo.getEffects().size());

            transactionStatus.setRollbackOnly();  // rollback, don't affect below test case
        });
    }

    @Test
    void givenBidirectionalOneToManyRelationship_andParentManagesChildCollectionInHashSet_andParentUtilMethodsAlignChildHashImproperly_andChildEqualsAndHashCodeIncludeParent___whenCallUtilMethodRemove___thenRemoveFail() {

        MonsterCard yuJyo = new MonsterCard();
        yuJyo.setCode("81332143");
        yuJyo.setName("YU-JYO");

        MonsterCardEffect shakeHand = new MonsterCardEffect();
        shakeHand.setIndex(1);
        shakeHand.setDescription("Send a hand shake invitation to opponent");

        log.info("shakeHand.hashCode before add = {}", shakeHand.hashCode());
        yuJyo.addEffect(shakeHand);
        log.info("shakeHand.hashCode after add = {}", shakeHand.hashCode());

        yuJyo.getEffects().contains(shakeHand);

        transactionTemplate.executeWithoutResult(transactionStatus -> {
            entityManager.persist(yuJyo);


            assertNotNull(yuJyo.getId());
            assertNotNull(shakeHand.getId());
            assertEquals(1, yuJyo.getEffects().size());

            log.info("shakeHand.hashCode before remove = {}", shakeHand.hashCode());
            yuJyo.removeEffect_SetParentNullBeforeRemoveFromHashSet(shakeHand);
            log.info("shakeHand.hashCode after remove = {}", shakeHand.hashCode());

            // the child is still in that collection
            assertEquals(1, yuJyo.getEffects().size());

            transactionStatus.setRollbackOnly();
        });

    }

}
