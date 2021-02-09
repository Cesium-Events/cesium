package events.cesium.kafka.delay.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;

import events.cesium.kafka.delay.CesiumConfig;
import events.cesium.kafka.delay.consumer.DelayConsumer;

public class DelayTrackerProducer extends KafkaProducer<Long, Long> {

    public DelayTrackerProducer(CesiumConfig cesiumConfig) {
        super(getKafkaConfig(cesiumConfig));
    }

    public static final Properties getKafkaConfig(CesiumConfig config) {
        Properties props = config.getProperties(DelayConsumer.class, "KafkaConfig");

        // These must be set to the ByteArrayDeserializer values. Override any value set
        // in config
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        return props;
    }
}