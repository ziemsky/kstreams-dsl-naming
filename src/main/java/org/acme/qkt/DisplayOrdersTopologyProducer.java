package org.acme.qkt;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.time.Duration;
import java.util.Properties;

@ApplicationScoped
public class DisplayOrdersTopologyProducer {

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

        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, CustomerEvent> customersById = builder.table(
            "customer-events-v1", // key: customerId
            Materialized.with(Serdes.String(), new ObjectMapperSerde<>(CustomerEvent.class))
        );

        final KStream<String, OrderEvent> ordersById = builder.stream(
                "order-events-v1",
                Consumed.with(
                    Serdes.String(), // key: orderId
                    new ObjectMapperSerde<>(OrderEvent.class)
                )
            )
            .filter((orderId, orderEvent) -> OrderEventType.RAISED.equals(orderEvent.eventType()))
            .peek((orderId, orderEvent) -> Log.infof("Consuming %s", orderId, orderEvent));

        final KStream<String, OrderEvent> ordersByCustomerId = ordersById
            .map((orderId, orderEvent) -> KeyValue.pair(orderEvent.customerId(), orderEvent))
            .repartition();

        final KStream<String, DisplayOrderEvent> displayOrdersByCustomerId =
            ordersByCustomerId.join(customersById,
                (customerId, orderEvent, customerEvent) -> new DisplayOrderEvent(orderEvent.orderId(), customerEvent.customerName()));

        final KStream<String, DisplayOrderEvent> displayOrdersByOrderId =
            displayOrdersByCustomerId.map(
                    (customerId, displayOrderEvent) -> KeyValue.pair(displayOrderEvent.orderId(), displayOrderEvent)
                )
                .repartition();

        displayOrdersByOrderId.to("display-orders-v1");

        final Topology topology = builder.build();

        Log.infof("Starting %s", topology.describe().toString());

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfig.applicationId());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapServers());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, kafkaConfig.defaultKeySerde());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, kafkaConfig.defaultKeySerde());
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, kafkaConfig.deserializationExceptionHandler());

        kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.setStateListener((newState, oldState) -> {
            Log.infof("Kafka Streams state change %s -> ", oldState, newState);

            if (KafkaStreams.State.RUNNING.equals(newState)) {
                kafkaStreams.start();
            }
        });
    }

    void stopStream(@Observes ShutdownEvent shutdownEvent) {
        kafkaStreams.close(Duration.ofSeconds(60));
    }
}