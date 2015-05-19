/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest;

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.event.EventBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.ByteBuffer;

// Helper class for dealing with the take and put operations in the specified
// channel.
class SpoolRecord {

  public final Integer attempt;
  public final Long timestamp;
  public final ProducerRecord<byte[], byte[]> payload;

  private final Event attemptEvent;
  private final Event timestampEvent;
  private final Event topicEvent;
  private final Event partitionEvent;
  private final Event keyEvent;
  private final Event valueEvent;

  // Helper method to take a record out of the specified channel.
  public static SpoolRecord take(FileChannel channel) {
    try {
      Event attemptEvent = channel.take();
      Event timestampEvent = channel.take();
      Event topicEvent = channel.take();
      Event partitionEvent = channel.take();
      Event keyEvent = channel.take();
      Event valueEvent = channel.take();
      if (attemptEvent != null &&
          timestampEvent != null &&
          topicEvent != null &&
          partitionEvent != null &&
          keyEvent != null &&
          valueEvent != null) {
        return new SpoolRecord(attemptEvent, timestampEvent, topicEvent,
                               partitionEvent, keyEvent, valueEvent);
      }
    } catch (ChannelException e) {
      // NOTE: What can be done at this point to mitigate the corrupted event?
      // In the case FileChannelConfiguration.FSYNC_PER_TXN is true, then
      // ChannelException will fire upon any corrupted event. What course of
      // action can be taken at this point? We force this to be false, hence
      // FileBackedTransaction.doTake will automatically skip over and find the
      // next available event without throwing this exception. Keeping this
      // catch-statement so compiler will not complain.
    }
    return null;
  }

  // Helper to construct a new spool record while bumping the attempt count.
  private SpoolRecord(Event attemptEvent, Event timestampEvent, Event topicEvent,
                      Event partitionEvent, Event keyEvent, Event valueEvent) {
    this.attempt = ByteBuffer.wrap(attemptEvent.getBody()).getInt() + 1;
    this.timestamp = ByteBuffer.wrap(timestampEvent.getBody()).getLong();
    byte[] partition = partitionEvent.getBody();
    this.payload = new ProducerRecord<byte[], byte[]>(new String(topicEvent.getBody()),
      (partition.length) == 0 ? null : ByteBuffer.wrap(partition).getInt(),
      keyEvent.getBody(), valueEvent.getBody());
    this.attemptEvent = EventBuilder.withBody(ByteBuffer.allocate(4).putInt(attempt).array());
    this.timestampEvent = timestampEvent;
    this.topicEvent = topicEvent;
    this.partitionEvent = partitionEvent;
    this.keyEvent = keyEvent;
    this.valueEvent = valueEvent;
  }

  // Construct a new spool record.
  public SpoolRecord(long timestamp, String topic, Integer partition, byte[] key, byte[] value) {
    this.attempt = 1;
    this.timestamp = timestamp;
    this.payload = new ProducerRecord<byte[], byte[]>(topic, partition, key, value);
    byte[] serializedAttempt = ByteBuffer.allocate(4).putInt(attempt).array();
    byte[] serializedTimestamp = ByteBuffer.allocate(8).putLong(timestamp).array();
    byte[] serializedTopic = topic.getBytes();
    byte[] serializedPartition = (partition == null) ? new byte[0] :
      ByteBuffer.allocate(4).putInt(partition.intValue()).array();
    this.attemptEvent = EventBuilder.withBody(serializedAttempt);
    this.timestampEvent = EventBuilder.withBody(serializedTimestamp);
    this.topicEvent = EventBuilder.withBody(serializedTopic);
    this.partitionEvent = EventBuilder.withBody(serializedPartition);
    this.keyEvent = EventBuilder.withBody(key);
    this.valueEvent = EventBuilder.withBody(value);
  }

  // Helper method to put a record into the specified channel.
  public void put(FileChannel channel) {
    channel.put(attemptEvent);
    channel.put(timestampEvent);
    channel.put(topicEvent);
    channel.put(partitionEvent);
    channel.put(keyEvent);
    channel.put(valueEvent);
  }
}
