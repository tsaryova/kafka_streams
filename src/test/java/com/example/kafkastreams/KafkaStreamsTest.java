package com.example.kafkastreams;

import com.example.kafkastreams.domain.Purchase;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest
@EmbeddedKafka(topics = {
        "purchases-topic",
        "masked-purchases",
        "electronics-purchases",
        "coffee-purchases",
        "popular-products"
})
public class KafkaStreamsTest {

    @Autowired
    private StreamsBuilder streamsBuilder;

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Purchase> inputTopic;
    private TestOutputTopic<String, Purchase> maskedPurchasesTopic;
    private TestOutputTopic<String, Purchase> electronicsTopic;
    private TestOutputTopic<String, Long> popularProductsTopic;

    @BeforeEach
    void setUp() {
        Topology topology = streamsBuilder.build();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(topology, props);

        JsonSerde<Purchase> purchaseSerde = new JsonSerde<>(Purchase.class);
        inputTopic = testDriver.createInputTopic(
                "purchases-topic",
                new StringSerializer(),
                new JsonSerializer<>()
        );

        maskedPurchasesTopic = testDriver.createOutputTopic(
                "masked-purchases",
                new StringDeserializer(),
                new JsonDeserializer<>(Purchase.class)
        );

        electronicsTopic = testDriver.createOutputTopic(
                "electronics-purchases",
                new StringDeserializer(),
                new JsonDeserializer<>(Purchase.class)
        );

        popularProductsTopic = testDriver.createOutputTopic(
                "popular-products",
                new StringDeserializer(),
                new LongDeserializer()
        );
    }

    @Test
    void testEmailMasking() {
        Purchase purchase = new Purchase(
                "order-123", "Alice", "alice@example.com", "Laptop", 999.99, "electronics"
        );
        inputTopic.pipeInput(purchase.orderId(), purchase);

        Purchase maskedPurchase = maskedPurchasesTopic.readRecord().value();
        assertThat(maskedPurchase.email()).isEqualTo("***masked***");
    }

    @Test
    void testStoreFiltering() {
        Purchase electronicsPurchase = new Purchase("order-1", "Bob", "bob@mail.com", "Phone", 599.99, "electronics");
        Purchase coffeePurchase = new Purchase("order-2", "Charlie", "charlie@mail.com", "Latte", 3.99, "coffee");

        inputTopic.pipeInput(electronicsPurchase.orderId(), electronicsPurchase);
        inputTopic.pipeInput(coffeePurchase.orderId(), coffeePurchase);

        assertThat(electronicsTopic.readRecord().value()).isEqualTo(electronicsPurchase);
        assertThat(electronicsTopic.isEmpty()).isTrue(); // Кофе не должно попасть сюда
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }
}
