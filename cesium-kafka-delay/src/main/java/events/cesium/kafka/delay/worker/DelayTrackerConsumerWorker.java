package events.cesium.kafka.delay.worker;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import events.cesium.kafka.delay.model.DelayEntry;
import events.cesium.kafka.delay.model.DelayKafkaConfig;

public class DelayTrackerConsumerWorker implements ConsumerRebalanceListener {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

//    private final CesiumConfig cesiumConfig;
    private final DelayKafkaConfig config;
    private final DelayQueue<DelayEntry> delayQueue;

    private final KafkaConsumer<byte[], byte[]> source;
    private final KafkaConsumer<byte[], byte[]> sourceSeeker;

    private final KafkaConsumer<Long, Long> trackerConsumer;
    private final KafkaProducer<Long, Long> trackerProducer;
    private final KafkaProducer<byte[], byte[]> destination;

    private final ReadWriteLock rwLock;

    public DelayTrackerConsumerWorker(DelayKafkaConfig config) {
        this.config = config;

        this.delayQueue = new DelayQueue<>();
        this.rwLock = new ReentrantReadWriteLock();

        log.debug("Initializing delay source consumer");
        source = new KafkaConsumer<>(config.getSrcConfig());
        sourceSeeker = new KafkaConsumer<>(config.getSrcSeekConfig());

        log.debug("Initializing delay tracking consumer");
        trackerConsumer = new KafkaConsumer<>(config.getTrackingConsumerConfig());

        log.debug("Initializing delay tracking producer");
        trackerProducer = new KafkaProducer<>(config.getTrackingProducerConfig());

        // Create the delay producer to short circuit any incoming requests that don't
        // have the correct metadata or are scheduled to dispatch in the past so we
        // don't create any unnecessary tracking messages.
        log.debug("Initializing delay destination producer");
        destination = new KafkaProducer<>(config.getDestConfig());
    }

    public void process() {
        // initialize the consumers (i.e. get src consumer partition assignment and
        // force assignment of those same partitions to the tracking consumer)
        log.info("Subscribing to delay source topic");
        source.subscribe(Collections.singletonList(config.getSourceTopic()), this);
        log.info("Subscribed to delay source topic {}", source.subscription());

        Executors.newSingleThreadExecutor().execute(new Runnable() {

            @Override
            public void run() {
                processSourceTopic();
            }
        });

        processDelayQueue();
    }

    public boolean processSourceTopic() {
        // start the consumers on a given queue with the correct
        // ConsumerRebalanceListener (on DelayConsumer to
        // update the assigned partitions on the delayTrackerConsumer
        while (true) {
            try {
                rwLock.readLock().lock();
                ConsumerRecords<byte[], byte[]> records = source.poll(Duration.ofMillis(10L));
                if (records.count() > 0) {
                    log.trace("Read [{}] records from delay source topic", records.count());
                }

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    DelayEntry delayEntry = getDelayEntry(record);

                    if (delayEntry == null || delayEntry.getDispatchTimestamp() <= System.currentTimeMillis()) {
                        // Short circuit and send directly to the producer
                        log.debug("Record [{}] has no delay. Sending to destination topic", record.offset());
                        destination.send(new ProducerRecord<byte[], byte[]>(config.getDestTopic(), record.key(),
                                record.value()));

                        // TODO: Do we need a marker in the tracking queue to know we've already sent
                        // TODO: it? Don't think so. Consumer will already have moved past that offset
                        // TODO: so it won't be "in the tracking system" side of things during
                        // TODO: rebalancing
                        continue;
                    }

                    log.debug("Adding record [{}] to the delay queue", record.offset());
                    delayQueue.add(delayEntry);

                    log.debug("Sending tracking record for [{}] to tracking topic [{}], partition [{}]",
                            record.offset(), config.getTrackingTopic(), record.partition());
                    trackerProducer.send(new ProducerRecord<Long, Long>(config.getTrackingTopic(), record.partition(),
                            System.currentTimeMillis(), delayEntry.getOffset(), delayEntry.getDispatchTimestamp()));
                }
            } finally {
                rwLock.readLock().unlock();
            }
        }
    }

    public void processDelayQueue() {
        while (true) {
            try {
                rwLock.readLock().lock();
                DelayEntry entry = delayQueue.poll(10, TimeUnit.MILLISECONDS);
                // Seek in the source consumer to the specified offset to get the record
                ConsumerRecord<byte[], byte[]> record = getRecordForEntry(entry);
                if (record != null) {
                    // send the record to the same partition on the destination producer
                    destination.send(new ProducerRecord<byte[], byte[]>(
                            config.getDestTopic(), record.key(), record.value()));

                    // Send a value with the positive offset to indicate the offset no longer needs
                    // to be set
                    trackerProducer.send(new ProducerRecord<Long, Long>(config.getTrackingTopic(), record
                            .partition(),
                            System.currentTimeMillis(), entry.getOffset(),
                            Math.negateExact(entry.getDispatchTimestamp())));

                }
            } catch (InterruptedException e) {
                // Thread was unexpectedly interrupted. set the interrupt flag and stop
                // processing
                Thread.currentThread().interrupt();
                return;
            } finally {
                rwLock.readLock().unlock();
            }
        }
    }

    private ConsumerRecord<byte[], byte[]> getRecordForEntry(DelayEntry entry) {
        TopicPartition topicPartition = new TopicPartition(
                config.getSourceTopic(), entry.getPartition());
        sourceSeeker.assign(Collections.singleton(topicPartition));
        sourceSeeker.seek(topicPartition, entry.getOffset());
        ConsumerRecords<byte[], byte[]> records = sourceSeeker.poll(Duration.ofMillis(10L));
        return records.records(topicPartition).stream().filter(record -> record.offset() == entry.getOffset())
                .findFirst().orElse(null);
    }

    /**
     * The delay information is optionally set in the record header which has a
     * "String"->byte[] mapping. There are two options for header values. The values
     * are expected to be 8 bytes representing a long primitive value. The
     * "delay-by" value delays by the specified number of milliseconds from when the
     * message is received. The "delay-until" message will delay the message until
     * the specified time in epoch milliseconds.
     * 
     * If neither header exists, no delay entry is returned.
     */
    private DelayEntry getDelayEntry(ConsumerRecord<byte[], byte[]> record) {
        Header delayBy = record.headers().lastHeader("delay-by");
        if (delayBy != null) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
            byteBuffer.put(delayBy.value());
            byteBuffer.flip();
            log.debug("Record [{}] is to be delayed by [{}]ms", record.offset(), byteBuffer.getLong());
            return new DelayEntry(record.partition(), record.offset(),
                    System.currentTimeMillis() + byteBuffer.getLong());
        }
        Header delayUntil = record.headers().lastHeader("delay-until");
        if (delayUntil != null) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
            byteBuffer.put(delayUntil.value());
            byteBuffer.flip();
            log.debug("Record [{}] is to be delayed until [{}] -> [{}]", record.offset(), byteBuffer
                    .getLong(),
                    new Date(byteBuffer.getLong()));
            return new DelayEntry(record.partition(), record.offset(), byteBuffer.getLong());
        }
        log.debug("Record [{}] does not have a delay header and will not be delayed", record.offset());
        return null;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // remove data in delayQueue from partitions that are no longer applicable
        // Block draining of the delay queue until this reinitialization is complete
        log.info("Acquiring lock to processing partition revoked");
        try {
            rwLock.writeLock().lock();

            List<Integer> revokedPartitionNumbers = partitions
                    .stream()
                    .map(partition -> partition.partition())
                    .collect(Collectors.toList());
            log.info("The following partitions were revoked from the source consumer {}", revokedPartitionNumbers);

            List<TopicPartition> newTrackerPartitions = source.assignment().stream().map(topic -> {
                return new TopicPartition(config.getTrackingTopic(), topic.partition());
            }).collect(Collectors.toList());

            log.info("Reassigning tracker consumer to partitions {}", newTrackerPartitions);
            trackerConsumer.assign(newTrackerPartitions);

            // Clean up any entries in the delay queue for any items associated with a
            // removed partitions
            List<DelayEntry> entriesToRemove = new ArrayList<>();
            for (DelayEntry entry : delayQueue) {
                if (revokedPartitionNumbers.contains(entry.getPartition())) {
                    entriesToRemove.add(entry);
                }
            }
            // Remove all the entries related to the removed partitions
            delayQueue.removeAll(entriesToRemove);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Processing source consumer partition assignment");
        try {
            // Block draining of the delay queue until this reinitialization is complete
            rwLock.writeLock().lock();

            List<Integer> newPartitionNumbers = partitions.stream().map(partition -> partition
                    .partition())
                    .collect(Collectors.toList());

            log.info("The folowing partitions were assigned to the source consumer {}", newPartitionNumbers);

            // Add the new partitions to the assignment the delay tracker consumer is
            // following
            List<TopicPartition> addedPartitions = newPartitionNumbers
                    .stream()
                    .map(partition -> new TopicPartition(config.getTrackingTopic(),
                            partition))
                    .collect(Collectors.toList());

            log.debug("Adding new partitions to tracker consumer {}", addedPartitions);

            // Get the current list of topic assignments and add the new ones
            Collection<TopicPartition> newPartitionList = trackerConsumer.assignment();
            newPartitionList.addAll(addedPartitions);

            log.info("Assigning the delay consumer partitions to {}", newPartitionList);
            trackerConsumer.assign(newPartitionList);

            // Figure out the last committed offset and the current offset so we can "catch up" on the new thread
            Map<TopicPartition, Long> startOffsets = trackerConsumer.beginningOffsets(addedPartitions);
            Map<TopicPartition, Long> currentOffsets = trackerConsumer.endOffsets(addedPartitions);

            // Iterate through the partitions one at a time to catch up on each one. The
            // numbers of partitions need to match between the delay topic and the incoming
            // topic. A message on the tracking topic is produced on the same partition as
            // the incoming message.
            for(Entry<TopicPartition, Long> entry : startOffsets.entrySet()) {
                log.debug("Catching up on partition [{}]", entry.getKey());
                long readOffset = entry.getValue();
                long endOffset = currentOffsets.entrySet().stream()
                        .filter(currentEntry -> currentEntry.getKey().partition() == entry.getKey().partition())
                        .map(currentEntry -> currentEntry.getValue())
                        .findFirst().orElse(readOffset);

                log.trace("Topic [{}], readOffset: {}, endOffset {}", entry.getKey(), readOffset, endOffset);

                // Need to create a specific consumer for the partition to read messages up to
                // at least the end offset and add those requests to the delay queue
                try (KafkaConsumer<Long, Long> rebalanceConsumer = new KafkaConsumer<>(
                        config.getTrackingConsumerSeekConfig())) {
                    // Assign the consumer to the specific topic/partition we're synchronizing
                    rebalanceConsumer.assign(Collections.singleton(entry.getKey()));

                    log.trace("Seeking [{}] to position [{}]", entry.getKey(), readOffset);
                    rebalanceConsumer.seek(entry.getKey(), readOffset);

                    log.trace("Catching up [{}] from [{}] to [{}]", entry.getKey(), readOffset, endOffset);
                    while (readOffset < endOffset) {
                        // It's okay if offset is greater than the end offset (records may have gotten
                        // into the queue since getting the latest offset above). This logic must proess
                        // until at least the endOffset
                        ConsumerRecords<Long, Long> records = rebalanceConsumer
                                .poll(Duration.of(10L, ChronoUnit.MILLIS));
                        for (ConsumerRecord<Long, Long> record : records) {
                            reprocessRecord(record);
                            readOffset = record.offset();
                        }
                    }
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    protected void reprocessRecord(ConsumerRecord<Long, Long> record) {
        // If the delayProcessTime is less than 0 it indicates the record
        // has been delayed and send to the destination producer
        if (record.value() <= 0) {
            processRecordComplete(record);
            return;
        }
        // Create a DelayEntry and queue it
        DelayEntry delayEntry = new DelayEntry(record.partition(), record.key(),
                Math.abs(record.value()));
        delayQueue.add(delayEntry);
        log.trace("Added DelayEntry to delay queue", delayEntry);
        return;
    }

    protected void processRecordComplete(ConsumerRecord<Long, Long> record) {
        // This is an indication the record was delivered after the delay so remove
        // it from the delay queue if it still exists there
        Optional<DelayEntry> foundEntry = delayQueue.stream()
                .filter(entry -> entry.getPartition() == record.partition()
                        && entry.getOffset() == record.key())
                .findFirst();
        if (foundEntry.isPresent()) {
            if (delayQueue.remove(foundEntry.get())) {
                log.trace("Removed DelayEntry from queue [{}]", foundEntry.get());

                // If a record was removed, see if the offset needs to be updated in the
                // consumer by
                // seeing if the lowest offset value within the partition is now higher than the
                // current offset
                OptionalLong minOffset = delayQueue.stream().filter(entry -> entry.getPartition() == record.partition())
                        .mapToLong(entry -> entry.getOffset()).min();
                if (minOffset.isPresent()) {
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    long currentPartitionOffset = trackerConsumer.position(topicPartition);
                    if (minOffset.getAsLong() > currentPartitionOffset) {
                        OffsetAndMetadata offsets = new OffsetAndMetadata(minOffset.getAsLong());
                        trackerConsumer.commitSync(Collections.singletonMap(topicPartition, offsets));
                        log.trace("Updated offset for partition [{}] from [{}] to [{}]", topicPartition,
                                currentPartitionOffset, minOffset.getAsLong());
                    } else {
                        log.trace("No need to update offset for partition [{}].  Current offset [{}]", topicPartition,
                                currentPartitionOffset);
                    }
                }
            }
        }
    }
}
