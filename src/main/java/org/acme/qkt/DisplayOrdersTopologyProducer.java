package org.acme.qkt;

import static org.acme.qkt.model.OrderEvent.OrderEventType.RAISED;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import org.acme.qkt.model.CustomerEvent;
import org.acme.qkt.model.DisplayOrderEvent;
import org.acme.qkt.model.OrderEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

@ApplicationScoped
public class DisplayOrdersTopologyProducer extends AbstractTopologyProducer {

    final static int PARTITIONS_COUNT = 1;

    public static final ForeachAction<String, Object> LOG_EVENTS =
        (key, event) -> Log.infof("Consuming %s", event);

    DisplayOrdersTopologyProducer(final KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    protected void buildStreamWith(final StreamsBuilder builder) {

        final KTable<String, CustomerEvent> customersById = builder
            .stream("customer-events-v1",
                Consumed.<String, CustomerEvent>as("customer-events-source")
                    .withKeySerde(Serdes.String()) // key: customerId
                    .withValueSerde(new ObjectMapperSerde<>(CustomerEvent.class))
            )
            .toTable(
                Named.as("customer-events-table"),
                Materialized.<String, CustomerEvent>as(Stores.persistentKeyValueStore("customersByIdTableStore"))
                    .withKeySerde(Serdes.String()) // key: customerId
                    .withValueSerde(new ObjectMapperSerde<>(CustomerEvent.class))
            );

        final KStream<String, OrderEvent> ordersById = builder
            .stream(
                "order-events-v1",
                Consumed
                    .<String, OrderEvent>as("order-events-source")
                    .withKeySerde(Serdes.String()) // key: orderId
                    .withValueSerde(new ObjectMapperSerde<>(OrderEvent.class))
            )
            .filter((orderId, orderEvent) -> RAISED.equals(orderEvent.eventType()), Named.as("filter-raised-orders-only"))
            .peek(LOG_EVENTS, Named.as("log-order-events"));

        final KStream<String, OrderEvent> ordersByCustomerId = ordersById
            .map((orderId, orderEvent) -> KeyValue.pair(
                    orderEvent.customerId(),
                    orderEvent
                ),
                Named.as("map-key-to-customerId")
            )
            .repartition(Repartitioned
                .<String, OrderEvent>as("ordersByCustomerId")
                .withKeySerde(Serdes.String()) // key: customerId
                .withValueSerde(new ObjectMapperSerde<>(OrderEvent.class))
                .withNumberOfPartitions(PARTITIONS_COUNT)
            );

        final KStream<String, DisplayOrderEvent> displayOrdersByCustomerId =
            ordersByCustomerId
                .join(
                    customersById,
                    (customerId, orderEvent, customerEvent) -> new DisplayOrderEvent(
                        orderEvent.orderId(), // key: customerId
                        customerEvent.customerName()
                    ),
                    Joined
                        .<String, OrderEvent, CustomerEvent>as("join-orders-to-customers")
                        .withKeySerde(Serdes.String()) // key: customerId
                        .withValueSerde(new ObjectMapperSerde<>(OrderEvent.class))
                );

        final KStream<String, DisplayOrderEvent> displayOrdersByOrderId =
            displayOrdersByCustomerId
                .map((customerId, displayOrderEvent) -> KeyValue.pair(
                        displayOrderEvent.orderId(),
                        displayOrderEvent
                    ),
                    Named.as("map-key-to-orderId"))
                .repartition(Repartitioned
                    .<String, DisplayOrderEvent>as("displayOrdersByOrderId")
                    .withKeySerde(Serdes.String()) // key: orderId
                    .withValueSerde(new ObjectMapperSerde<>(DisplayOrderEvent.class))
                    .withNumberOfPartitions(PARTITIONS_COUNT)
                );

        displayOrdersByOrderId
            .peek(LOG_EVENTS, Named.as("log-display-order-events"))
            .to(
                "display-order-events-v1",
                Produced.<String, DisplayOrderEvent>as("display-orders-sink")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(new ObjectMapperSerde<>(DisplayOrderEvent.class))
            );
    }

    @Override
    protected String topologyName() {
        return "display-order-events";
    }
}
