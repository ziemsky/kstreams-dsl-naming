package org.acme.qkt;

import io.smallrye.config.ConfigMapping;

import java.nio.file.Path;
import java.util.Optional;

@ConfigMapping(prefix = "custom.kafka")
public interface KafkaConfig {

    String bootstrapServers();

    String applicationId();

    String defaultKeySerde();

    String defaultValueSerde();

    String deserializationExceptionHandler();

    Long commitIntervalMs();

    Optional<Boolean> saveTopologyToFile();

    Optional<Path> stateDir();

    Optional<Long> stateStoreCacheMaxBytes();
}
