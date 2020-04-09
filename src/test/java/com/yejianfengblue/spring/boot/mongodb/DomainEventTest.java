package com.yejianfengblue.spring.boot.mongodb;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.AfterDomainEventPublication;
import org.springframework.data.domain.DomainEvents;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.stereotype.Service;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Before run this test, start a clean MongoDB on localhost:27017
 */
@SpringBootTest
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@EnableMongoRepositories(considerNestedRepositories = true)
@Slf4j
class DomainEventTest {

    @Document("DomainEventTestUser")
    @Getter
    @ToString
    private static class Order {

        @Id
        @EqualsAndHashCode.Include
        private String id;

        private Status status;

        Order() {

            this.status = Status.PAYMENT_EXPECTED;
        }

        @Transient
        private transient final List<Object> domainEvents = new ArrayList<>();

        /**
         * All domain events currently captured by the aggregate.
         */
        @DomainEvents
        protected Collection<Object> domainEvents() {

            log.info("Publish domain events: {}", domainEvents);
            return Collections.unmodifiableList(domainEvents);
        }

        /**
         * Registers the given event object for publication on a call to a Spring Data repository's save methods.
         *
         * @param event must not be {@literal null}.
         * @return the event that has been added.
         */
        protected <T> T registerEvent(T event) {

            Assert.notNull(event, "Domain event must not be null!");

            this.domainEvents.add(event);
            log.info("Event {} registered", event);
            return event;
        }

        /**
         * Clears all domain events currently held. Usually invoked by the infrastructure in place in Spring Data
         * repositories.
         */
        @AfterDomainEventPublication
        protected void clearDomainEvents() {

            this.domainEvents.clear();
            log.info("Events are cleared");
        }

        /**
         * Mark this order as paid, and register a {@link OrderPaidEvent}.
         * @return
         */
        Order markPaid() {

            if (isPaid()) {
                throw new IllegalStateException(
                        String.format("Already paid order cannot be paid again! " +
                                        "Order ID is %s. Current status is %s",
                                this.id, this.status));
            } else {
                this.status = Status.PAID;
                registerEvent(new OrderPaidEvent(this.id));
                return this;
            }
        }

        Order markCompleted() {

            if (isCompleted()) {
                throw new IllegalStateException(
                        String.format("Already completed order cannot be completed again! Order ID is %s", this.id));
            } else {
                this.status = Status.COMPLETED;
                return this;
            }
        }

        boolean isPaid() {
            return !this.status.equals(Status.PAYMENT_EXPECTED);
        }

        boolean isCompleted() {
            return this.status.equals(Status.COMPLETED);
        }

        public static enum Status {

            PAYMENT_EXPECTED,

            PAID,

            COMPLETED;
        }
    }

    interface OrderRepository extends MongoRepository<Order, String> {}

    @Autowired
    private OrderRepository orderRepository;

    @Getter
    @RequiredArgsConstructor
    @ToString
    private static class OrderPaidEvent {

        private final String orderId;
    }

    @Service
    @RequiredArgsConstructor
    @Slf4j
    private static class OrderEventHandler{

        @NonNull
        private final OrderRepository orderRepository;

        private final Set<Order> ordersInProgress = Collections.newSetFromMap(new ConcurrentHashMap<Order, Boolean>());

        @TransactionalEventListener
        void handleUserRegistrationEvent(OrderPaidEvent orderPaidEvent) {

            log.info("Receive OrderPaidEvent {}", orderPaidEvent);

            String orderId = orderPaidEvent.getOrderId();
            Order order = orderRepository.findById(orderId)
                    .orElseThrow(() -> new IllegalStateException(String.format("No order found for ID %s", orderId.toString())));

            log.info("Processing Order {}", orderId);
            // do some process work here... is omitted

            order = orderRepository.save(order.markCompleted());
            ordersInProgress.remove(order);
            log.info("Order {} completed", orderId);
        }
    }

    @TestConfiguration
    @Import(OrderEventHandler.class)
    static class config { }

    @Test
    @SneakyThrows
    void t() {

            Order order = new Order();
            order = orderRepository.save(order);
            // payment work here... is omitted
            order = orderRepository.save(order.markPaid());
    }
}
