package events.cesium.kafka.delay.model;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

public class DelayKafkaConfig {

    private final String groupId;

    private final String srcTopic;
    private final Properties srcConfig;

    private final String destTopic;
    private final Properties destConfig;

    private final String trackingTopic;
    private final Properties trackingConfig;

    /**
     * This class encapsulates kafka properties for the source topic and the
     * destination topic to send to after the delay has been introduced. It also
     * encapsulates the configuration for the tracking topic used to track and
     * introduce the delay. The class contains methods to return versions of the
     * config overridden with required values.
     * 
     * The groupId is used for group reads of the src consumers as well as the
     * tracking consumers. It must be a unique value not used by other consumers of
     * the source topic and the tracking topic to ensure the delay works correctly.
     * 
     * A minimum config must include at least bootstrap.servers
     */
    public DelayKafkaConfig(String groupId, String srcTopic, Properties srcConfig, String destTopic,
            Properties destConfig, String trackingTopic, Properties trackingConfig) {
        this.groupId = groupId;

        this.srcTopic = srcTopic;
        this.srcConfig = srcConfig;

        this.destTopic = destTopic;
        this.destConfig = destConfig;

        this.trackingTopic = trackingTopic;
        this.trackingConfig = trackingConfig;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getSourceTopic() {
        return srcTopic;
    }

    /**
     * The properties for the source configuration. This overrides the key SerDe
     * values to be byte arrays
     * 
     * @return
     */
    public Properties getSrcConfig() {
        Properties props = new Properties(srcConfig);

        // These must be set for consumers to read data as a byte array directly without
        // further interpreting the data
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // Set the group ID for consumer
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return props;
    }

    /**
     * Same as getSourceConfig except no group.id value set
     */
    public Properties getSrcSeekConfig() {
        Properties props = getSrcConfig();
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        return props;
    }

    public String getDestTopic() {
        return destTopic;
    }

    public Properties getDestConfig() {
        Properties props = new Properties(destConfig);

        // These must be set for producers to write data as a byte array directly
        // without further interpreting the data
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return props;
    }

    public String getTrackingTopic() {
        return trackingTopic;
    }

    public Properties getTrackingConsumerConfig() {
        Properties props = new Properties(trackingConfig);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        // The implementation depends on explicit coordination of offset commits
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());

        // Set the group ID for consumer
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return props;
    }

    /**
     * Same as getTrackingConsumerConfig except no group.id value set
     */
    public Properties getTrackingConsumerSeekConfig() {
        Properties props = getTrackingConsumerConfig();
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        return props;
    }

    public Properties getTrackingProducerConfig() {
        Properties props = new Properties(trackingConfig);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        return props;
    }
}
