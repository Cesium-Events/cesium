package events.cesium.kafka.delay.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import events.cesium.kafka.delay.CesiumConfig;

public class DelayTrackerConsumer extends KafkaConsumer<Long, Long> {

    public DelayTrackerConsumer(CesiumConfig cesiumConfig) {
        super(getKafkaConfig(cesiumConfig));
    }

    private static final Properties getKafkaConfig(CesiumConfig config) {
        Properties props = config.getProperties(DelayTrackerConsumer.class, "KafkaConfig");

        // These must be set to the ByteArrayDeserializer values. Override any value set
        // in config
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        // The implementation depends on explicit coordination of offset commits
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        return props;
    }
}
