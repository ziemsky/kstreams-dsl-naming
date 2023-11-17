package org.acme.qkt;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;

@ApplicationScoped
public class DisplayOrdersTopologyProducer {

    public static final String TOPOLOGY_NAME = "display-order-events";
    public static final int PARTITIONS_COUNT = 1;

    private final KafkaConfig kafkaConfig;
    KafkaStreams kafkaStreams;

    DisplayOrdersTopologyProducer(final KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
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


    public void buildStream(@Observes StartupEvent startupEvent) {

        Log.infof("Building %s topology", TOPOLOGY_NAME);

        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, CustomerEvent> customersById = builder
            .stream("customer-events-v1",
                Consumed.with(
                    Serdes.String(), // key: customerId
                    new ObjectMapperSerde<>(CustomerEvent.class)
                )
            )
            .peek((customerId, customerEvent) -> Log.infof("Consuming %s", customerEvent))
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

        final Topology topology = builder.build();

        Log.infof("%s %s", TOPOLOGY_NAME, topology.describe().toString());

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfig.applicationId());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapServers());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, kafkaConfig.defaultKeySerde());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, kafkaConfig.defaultKeySerde());
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, kafkaConfig.deserializationExceptionHandler());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.setStateListener((newState, oldState) -> {
            Log.infof("%s topology state change %s -> %s", TOPOLOGY_NAME, oldState, newState);
        });

        Log.infof("%s topology START", TOPOLOGY_NAME);
        kafkaStreams.start();
    }

    void stopStream(@Observes ShutdownEvent shutdownEvent) {
        kafkaStreams.close(Duration.ofSeconds(60));
    }
}