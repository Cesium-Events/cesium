# cesium-kafka-delay

This project implements a kafka relay that reads messages from a source topic and writes them to a destination topic at scale.  The relay can optionally introduce a fixed delay or relay the topic at a set time.

## Uses
Re-ordering events goes against the grain when thinking about data streams and Kafka, so there may be limited use case for this functionality.  That said, there are situations where a triggering event happens at time A, but the resulting action need to happen at a later time.  While a message consumer can implement that delay logic at the client level, offloading this functionality has advantages as durable scheduling at scale can be complex.

This project attempts to implement that complexity and presenting it as Kafka streams.

### Simple Use Case
Given the use case where a consumer reads from the topic userSignups.  The event consumer is in charge of sending an email 24 hours after the user initially signed up.  The consumer process then sends a new event to the delayedSignupNotice topic and sets some metadata fields to indicate a 24 hour delay should be applied to the message.

The delayedSignupNotice topic is configured as a delay topic usng cesium-kafka-delay.

The cesium-kafka-delay process will then wait 24 hours before forwarding that event to the configured sendSignupNotice topic where a consumer that actually delivers the intended email.

While this use case could could have been implemented using a cron or other sheduled batch job, it could only do so by batching multiple events to trigger at the same time (i.e. when the cron job ran).  For this use case it would even make sense to send an email "the morning after signup" rather than exactly 24 hours after signup, so maybe we need a better use case :)

But in a "kafka world", there are advantages to stream all the events instead of introducing batch semantics to interrupt the flows.

The functionality offered in this implementation is intended as an option.

## How it Works
This project was inspired by the work done on KMQ: https://github.com/softwaremill/kmq

While KMQ has a different goal if mimicking Amazon SQS, the core ability to reliably redeliver messages out of order is critical to this project.

The delay queue implementation uses three queues.  There is a source queue, a destination queue, and a tracking queue.  The tracking queue is only used by this project.  This project only consumes from the source and only produces messages to the destination queue.

At a high level, when a record is consumed from the source, the record headers are read, searching for a cesium-specific header:
- *cesium-delay-by* -> Delay by the specified number of milliseconds
- *cesium-delay-until* -> Delay message until the specified time (in epoch milliseconds)

If the record does not contain one of the headers or the cesium-delay-until time has passed, the record is sent directly to the destination topic and processing is complete.

Otherwise a new message is written to the tracking topic.  This message will be written on the same partion the message was consumed on.  The message itself will have a key of the offset of the message and a value of the long epoch milliseconds timestamp indicating when to relay the message to the destination topic.

At the same time this same information will be stored in memory in a DelayQueue that will monitor fo rthe events to be triggered.  When the even is triggered by the DelayQueue, a client for the source topic will seek to the partition and offset of the triggered message and relay that message to the destination queue before then writing a message to the tracking queue.  That tracking message will be written to the same partition but have a negative value for the offset to indicate the message was delivered.

The offsets on the tracking queue are set programmatically so on process restart, the offsets will be on the oldest unsent message.  The consumers can read the tracking stream to the latest message in order to rebuild the delayQueue in memory by adding entries with positive offsets and then removing them if it reads an entry with a negative offset of the same value.  This also happens when partitions are rebalanced, so the delay queue is properly rebuilt.

# Project Status
This pre-release project is very much a hobby and proof of concept at this point.  It is used to explore usage of Kafka and the possibilites of building an event scheduler at scale.  It definitely needs cleanup, but comments and suggestions are welcome.  There are no current plans to make this an actual product.

*Expect Bugs!*
