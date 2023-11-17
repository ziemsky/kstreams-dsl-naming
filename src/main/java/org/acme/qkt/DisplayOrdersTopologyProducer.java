package org.acme.qkt;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

@ApplicationScoped
public class DisplayOrdersTopologyProducer extends AbstractTopologyProducer {

    final static int PARTITIONS_COUNT = 1;

    public static final ForeachAction<String, CustomerEvent> LOG_CONSUMING =
        (key, event) -> Log.infof("Consuming %s", event);

    DisplayOrdersTopologyProducer(final KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

    record CustomerEvent(
        String customerId,
        String customerName
    ){}

    record OrderEvent(
        String orderId,
        OrderEventType eventType,
        String customerId
    ){}

    enum OrderEventType {
        UPDATED, // order first created or subsequently updated
        RAISED   // order submitted for processing
    }

    record DisplayOrderEvent(
        String orderId,
        String customerName
    ){}

    @Override
    protected void buildStreamWith(final StreamsBuilder builder) {

        final KTable<String, CustomerEvent> customersById = builder
            .stream("customer-events-v1",
                Consumed.with(
                    Serdes.String(), // key: customerId
                    new ObjectMapperSerde<>(CustomerEvent.class)
                )
            )
            .peek(LOG_CONSUMING)
            .toTable(
                Named.as("customersByIdTable"),
                Materialized.<String, CustomerEvent>as(Stores.persistentKeyValueStore("customersByIdTableStore"))
                    .withKeySerde(Serdes.String()) // key: customerId
                    .withValueSerde(new ObjectMapperSerde<>(CustomerEvent.class))
            );

        final KStream<String, OrderEvent> ordersById = builder.stream(
                "order-events-v1",
                Consumed.with(
                    Serdes.String(), // key: orderId
                    new ObjectMapperSerde<>(OrderEvent.class)
                )
            )
            .filter((orderId, orderEvent) -> OrderEventType.RAISED.equals(orderEvent.eventType()))
            .peek((orderId, orderEvent) -> Log.infof("Consuming %s", orderEvent));

        final KStream<String, OrderEvent> ordersByCustomerId = ordersById
            .map((orderId, orderEvent) -> KeyValue.pair(
                orderEvent.customerId(),
                orderEvent
            ))
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
                        orderEvent.orderId(),
                        customerEvent.customerName()
                    )
                );

        final KStream<String, DisplayOrderEvent> displayOrdersByOrderId =
            displayOrdersByCustomerId
                .map((customerId, displayOrderEvent) -> KeyValue.pair(
                    displayOrderEvent.orderId(),
                    displayOrderEvent
                ))
                .repartition(Repartitioned
                    .<String, DisplayOrderEvent>as("displayOrdersByOrderId")
                    .withKeySerde(Serdes.String()) // key: orderId
                    .withValueSerde(new ObjectMapperSerde<>(DisplayOrderEvent.class))
                    .withNumberOfPartitions(PARTITIONS_COUNT)
                );

        displayOrdersByOrderId.to("display-order-events-v1");
    }

    @Override
    protected String topologyName() {
        return "display-order-events";
    }
}
