package org.acme.qkt;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

@ApplicationScoped
public class TopologyProducerB {
    
    @Produces
    public Topology topologyB() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        
        streamsBuilder
            .stream("topic-b", Consumed.with(Serdes.String(), Serdes.String()))

            .foreach((key, value) -> Log.infof("Consuming from topic-b %s : %s", key, value));
        
        return streamsBuilder.build();
    }
}
