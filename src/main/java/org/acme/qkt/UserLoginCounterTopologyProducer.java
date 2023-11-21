package org.acme.qkt;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import org.acme.qkt.model.LoginCountEvent;
import org.acme.qkt.model.LoginEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@ApplicationScoped
public class UserLoginCounterTopologyProducer extends AbstractTopologyProducer {

    UserLoginCounterTopologyProducer(final KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    protected void buildStreamWith(final StreamsBuilder builder) {

        builder
            .stream(
                "login-events-v1",
                Consumed.with(
                    Serdes.String(),
                    new ObjectMapperSerde<>(LoginEvent.class)
                )
            )

            .peek(LOG_EVENTS)

            .groupByKey(
                Grouped.with(
                    Serdes.String(),
                    new ObjectMapperSerde<>(LoginEvent.class)
                )
            )

            .aggregate(
                () -> new LoginCountEvent(null, 0L),
                (userId, incomingLoginEvent, existingLoginCount) -> new LoginCountEvent(userId, existingLoginCount.count() + 1),
                Materialized.<String, LoginCountEvent, KeyValueStore<Bytes, byte[]>>as("KSTREAM-AGGREGATE-STATE-STORE-0000000002")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(new ObjectMapperSerde<>(LoginCountEvent.class))
            )

            .toStream()

            .peek(LOG_EVENTS)

            .to(
                "login-count-events-v1",
                Produced.with(
                    Serdes.String(),
                    new ObjectMapperSerde<>(LoginCountEvent.class)
                )
            );
    }

    @Override
    protected String topologyName() {
        return "user-login-events";
    }
}
