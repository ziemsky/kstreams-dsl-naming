package org.acme.qkt;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;

abstract public class AbstractTopologyProducer {

    public static final ForeachAction<String, Object> LOG_EVENTS =
        (key, event) -> Log.infof("Consuming %s", event);

    public static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

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
        final String topologyOut = topology.describe().toString();

        saveToFile(topologyOut);

        Log.infof("%s %s", topologyName(), topologyOut);

        kafkaStreams = new KafkaStreams(topology, kafkaConfig());

        kafkaStreams.setStateListener((newState, oldState) ->
            Log.infof("%s topology state change: %s -> %s", topologyName(), oldState, newState));

        Log.infof("%s topology START", topologyName());

        kafkaStreams.start();
    }

    private Properties kafkaConfig() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfig.applicationId());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapServers());
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, kafkaConfig.defaultKeySerde());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, kafkaConfig.defaultKeySerde());
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, kafkaConfig.deserializationExceptionHandler());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, kafkaConfig.commitIntervalMs());
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir().toString());
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, kafkaConfig.stateStoreCacheMaxBytes().orElse(0L));

        return properties;
    }

    private Path stateDir() {

        return Path.of(
            kafkaConfig.stateDir().orElse(Path.of(TEMP_DIR)).toString(),
            "kafka-streams",
            topologyName()
        );
    }

    abstract protected String topologyName();

    protected abstract void buildStreamWith(final StreamsBuilder builder);

    void stopStream(@Observes ShutdownEvent shutdownEvent) {
        Log.infof("%s topology STOP", topologyName());
        kafkaStreams.close(Duration.ofSeconds(60));
    }

    private void saveToFile(final String topologyOut) {
        kafkaConfig.saveTopologyToFile().filter(isSave -> isSave).ifPresent(save -> {
            final Path topologyFilePath = Paths.get(TEMP_DIR, topologyName() + ".txt");
            try {
                Files.writeString(topologyFilePath, topologyOut, CREATE, TRUNCATE_EXISTING);
                Log.infof("%s topology saved in %s", topologyName(), topologyFilePath);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to save topology file %s".formatted(topologyFilePath), e);
            }
        });
    }
}