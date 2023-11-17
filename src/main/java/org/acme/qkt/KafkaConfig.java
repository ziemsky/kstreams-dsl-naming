package org.acme.qkt;

import io.smallrye.config.ConfigMapping;

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
}
