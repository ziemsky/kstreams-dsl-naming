package org.acme.qkt;

import org.apache.kafka.common.serialization.*;

// To work around the fact that Quarkus fails to find org.apache.kafka.common.serialization.Serdes.StringSerde
// at startup
@SuppressWarnings("unused")
public class StringSerde extends Serdes.WrapperSerde<String> {

    public StringSerde() {
        super(new StringSerializer(), new StringDeserializer());
    }
}
