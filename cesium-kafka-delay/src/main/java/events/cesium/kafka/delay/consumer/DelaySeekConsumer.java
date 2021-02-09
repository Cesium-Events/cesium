package events.cesium.kafka.delay.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import events.cesium.kafka.delay.CesiumConfig;

public class DelaySeekConsumer extends KafkaConsumer<byte[], byte[]> {

    public DelaySeekConsumer(CesiumConfig cesiumConfig) {
        super(getKafkaConfig(cesiumConfig));
    }

    private static final Properties getKafkaConfig(CesiumConfig config) {
        Properties props = config.getProperties(DelayConsumer.class, "KafkaConfig");

        // These must be set to the ByteArrayDeserializer values. Override any value set
        // in config
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        return props;
    }
}
