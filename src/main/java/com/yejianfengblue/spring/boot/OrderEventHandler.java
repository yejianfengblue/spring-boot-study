package com.yejianfengblue.spring.boot;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

//@Service
@RequiredArgsConstructor
@Slf4j
class OrderEventHandler {

    @NonNull
    private final OrderRepository orderRepository;

    private final Set<Order> ordersInProgress = Collections.newSetFromMap(new ConcurrentHashMap<Order, Boolean>());

//    @Async
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
