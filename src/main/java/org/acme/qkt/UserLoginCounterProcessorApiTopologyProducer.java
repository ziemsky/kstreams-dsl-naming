package org.acme.qkt;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;
import org.acme.qkt.model.LoginCountEvent;
import org.acme.qkt.model.LoginEvent;
import org.apache.kafka.common.requests.StopReplicaResponse;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;

import java.util.Map;

public class UserLoginCounterProcessorApiTopologyProducer {

    protected void buildTopology() {

        Topology topology = new Topology();

        topology
            .addSource(
                "login-events-source",                              // processor name
                new StringDeserializer(),
                new ObjectMapperDeserializer<>(LoginEvent.class),
                "login-events-v1"                                   // topic
            )

            .addStateStore(
                Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore("login-counts-state-store"),
                    Serdes.String(),
                    new ObjectMapperSerde<>(LoginCountEvent.class)
                ).withLoggingEnabled(
                    Map.of() // changelog topic parameters
                ),
                "accumulate-login-counts-by-userId"     // processor(s) allowed to use our state store
            )

            .addProcessor(
                "accumulate-login-counts-by-userId",    // processor name
                () -> new Processor<String, LoginEvent, String, LoginCountEvent>() {

                    private ProcessorContext<String, LoginCountEvent> context;
                    private KeyValueStore<String, LoginCountEvent> stateStore;

                    @Override public void init(final ProcessorContext<String, LoginCountEvent> context) {
                        this.context = context;
                        stateStore = this.context.getStateStore("login-counts-state-store");
                    }

                    @Override public void process(final Record<String, LoginEvent> record) {

                        final LoginCountEvent previousloginCountEvent = stateStore.get(record.key());

                        final LoginCountEvent newLoginCountEvent = new LoginCountEvent(record.key(), previousloginCountEvent.count() + 1);

                        stateStore.put(record.key(), newLoginCountEvent);

                        context.forward(record.withValue(newLoginCountEvent));
                    }
                },
                "login-events-source"                       // parent processor name
            )

            .addSink(
                "login-counts-sink",                        // processor name
                "login-count-events-v1",                    // topic name
                new StringSerializer(),
                new ObjectMapperSerializer<>(),
                "accumulate-login-counts-by-userId"         // parent processor name
            )
        ;
    }
}
