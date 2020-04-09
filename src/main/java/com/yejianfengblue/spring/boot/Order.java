package com.yejianfengblue.spring.boot;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.AfterDomainEventPublication;
import org.springframework.data.domain.DomainEvents;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Document("DomainEventTestUser")
@Getter
@ToString
@Slf4j
public class Order {

    @Id
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
     *
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