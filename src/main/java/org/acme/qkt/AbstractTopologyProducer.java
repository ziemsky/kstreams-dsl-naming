package org.acme.qkt;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.streams.*;

import java.time.Duration;
import java.util.Properties;

abstract public class AbstractTopologyProducer {

    private KafkaConfig kafkaConfig;

    private KafkaStreams kafkaStreams;

    private AbstractTopologyProducer() {
        // to keep Quarkus ArC happy
    }

    protected AbstractTopologyProducer(final KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public void buildStream(@Observes StartupEvent startupEvent) {

        Log.infof("%s topology BUILD", topologyName());

        final StreamsBuilder builder = new StreamsBuilder();

        buildStreamWith(builder);

        final Topology topology = builder.build();

        Log.infof("%s %s", topologyName(), topology.describe().toString());

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfig.applicationId());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapServers());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, kafkaConfig.defaultKeySerde());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, kafkaConfig.defaultKeySerde());
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, kafkaConfig.deserializationExceptionHandler());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, kafkaConfig.commitIntervalMs());

        kafkaStreams = new KafkaStreams(topology, properties);


        kafkaStreams.setStateListener((newState, oldState) ->
            Log.infof("%s topology state change: %s -> %s", topologyName(), oldState, newState));

        Log.infof("%s topology START", topologyName());

        kafkaStreams.start();
    }

    abstract protected String topologyName();

    protected abstract void buildStreamWith(final StreamsBuilder builder);

    void stopStream(@Observes ShutdownEvent shutdownEvent) {
        Log.infof("%s topology STOP", topologyName());
        kafkaStreams.close(Duration.ofSeconds(60));
    }
}