package com.yejianfengblue.spring.boot.mongodb;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.event.LoggingEventListener;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Before run this test, start a clean MongoDB on localhost:27017
 */
@SpringBootTest
@EnableMongoRepositories(considerNestedRepositories = true, basePackageClasses = AggregationTest.class)
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class})
@Slf4j
public class AggregationTest {

    @Document("AggregationTestOrder")
    @Data
    @AllArgsConstructor(onConstructor_ = @PersistenceConstructor)
    private static class Order {

        private String id;

        private String customerName;

        private LocalDateTime orderTime;

        private List<LineItem> lineItems;

        Order(String customerName, LocalDateTime orderTime) {
            this(null, customerName, orderTime, new ArrayList<>());
        }

        Order addItem(LineItem lineItem) {

            this.lineItems.add(lineItem);
            return this;
        }
    }

    @Data
    @RequiredArgsConstructor(onConstructor_ = @PersistenceConstructor)
    private static class LineItem {

        private final String name;

        private final int price;

        private int quantity = 1;

        LineItem(String name, int price, int quantity) {

            this(name, price);
            this.quantity = quantity;
        }
    }

    @Value
    private static class Invoice {

        private final String orderId;

        private final Integer taxAmount;

        private final Integer netAmount;

        private final Integer totalAmount;

        private final List<LineItem> lineItems;
    }

    interface AggregationTestOrderRepository extends MongoRepository<Order, String> {

        @Aggregation("{ $group : { " +
                "_id : $customerName, " +
                "count : { $sum : 1 } " +
                "} }")
        List<OrderCountPerCustomer> orderCountPerCustomer(Sort sort);
    }

    @Autowired
    private AggregationTestOrderRepository orderRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Value
    private static class OrderCountPerCustomer {

        @Id  // because mongodb group operation requires a field named "_id", annotated with @Id makes it mapped to this class field
        private String customerName;

        private Integer count;
    }

    @TestConfiguration  // class access modifier must not be private
    static class Config {

        @Bean
        LoggingEventListener mongoEventListener() {
            return new LoggingEventListener();
        }
    }

    @Test
    void MongoRepositoryAggregationAnnotationTest() {

        Order berryOrder = new Order("Berry Magician Girl", LocalDateTime.now())
                .addItem(new LineItem("Berry Juice", 10));
        Order lemonOrder = new Order("Lemon Magician Girl", LocalDateTime.now())
                .addItem(new LineItem("Lemon Juice", 20));
        Order appleOrder = new Order("Apple Magician Girl", LocalDateTime.now())
                .addItem(new LineItem("Apple Juice", 30));
        Order hotChocolateOrder = new Order("Chocolate Magician Girl", LocalDateTime.now())
                .addItem(new LineItem("Hot Chocolate", 40));
        Order iceChocolateOrder = new Order("Chocolate Magician Girl", LocalDateTime.now())
                .addItem(new LineItem("Ice Chocolate", 40));
        Order kiwiOrder = new Order("Kiwi Magician Girl", LocalDateTime.now())
                .addItem(new LineItem("Kiwi Juice", 50));

        orderRepository.saveAll(List.of(berryOrder, lemonOrder, appleOrder, hotChocolateOrder, iceChocolateOrder, kiwiOrder));

        List<OrderCountPerCustomer> orderCountPerCustomerList = orderRepository.orderCountPerCustomer(Sort.by(Sort.Order.desc("count")));
        assertThat(orderCountPerCustomerList).hasSize(5);
        assertThat(orderCountPerCustomerList.get(0))
                .isEqualTo(new OrderCountPerCustomer("Chocolate Magician Girl", 2));

        orderRepository.deleteAll();
    }

    @Test
    void MongoTemplateAggregationSupportTest() {

        final double taxRate = 0.1;

        Order berryOrder = new Order("Berry Magician Girl", LocalDateTime.now())
                .addItem(new LineItem("Berry Juice", 10));

        berryOrder = orderRepository.save(berryOrder);

        AggregationResults<Invoice> invoiceAggregationResults = mongoTemplate.aggregate(
                newAggregation(Order.class, // TypeAggregation
                        match(where("id").is(berryOrder.getId())),  // use class field name
                        unwind("lineItems"),  //
                        // project to (id, customerName, lineItems, priceTotal)
                        project("id", "customerName", "lineItems")
                                .andExpression("'$lineItems.price' * '$lineItems.quantity'").as("priceTotal"),  // SpEL
                        group("id")
                                .sum("priceTotal").as("netAmount")
                                .addToSet("lineItems").as("lineItems"),
                        project("lineItems", "netAmount")
                                .and("orderId").previousOperation()
                                .andExpression("netAmount * [0]", taxRate).as("taxAmount")
                                .andExpression("netAmount * (1 + [0])", taxRate).as("totalAmount")
                ),
                Invoice.class);// output type

        Invoice invoiceForBerryOrder = invoiceAggregationResults.getUniqueMappedResult();
        assertThat(invoiceForBerryOrder.getOrderId()).isEqualTo(berryOrder.getId());
        assertThat(invoiceForBerryOrder.getLineItems()).isEqualTo(berryOrder.getLineItems());
        assertThat(invoiceForBerryOrder.getTaxAmount()).isEqualTo(1);
        assertThat(invoiceForBerryOrder.getNetAmount()).isEqualTo(10);
        assertThat(invoiceForBerryOrder.getTotalAmount()).isEqualTo(11);
    }
}
