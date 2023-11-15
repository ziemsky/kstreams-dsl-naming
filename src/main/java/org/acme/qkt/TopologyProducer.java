package org.acme.qkt;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

@ApplicationScoped
public class TopologyProducer {

    record MyJsonObject(String payloadValue){}

    @Produces
    public Topology topology() {

        StreamsBuilder builder = new StreamsBuilder();

        builder
            .stream("topic-a", Consumed.with(Serdes.String(), new ObjectMapperSerde<>(MyJsonObject.class)))

            .foreach((key, value) -> Log.infof("Consuming topic-a %s : %s", key, value));

        final Topology topology = builder.build();

        Log.infof("Starting %s", topology.describe().toString());

        return topology;
    }
}
