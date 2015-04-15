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
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import io.confluent.kafkarest.entities.SpoolMode;

import com.codahale.metrics.MetricRegistry;

public class SpoolProducer<K, V> extends KafkaProducer<K, V> {

  private static final Logger log = LoggerFactory.getLogger(SpoolProducer.class);

  private static final MetricRegistry metrics = new MetricRegistry();

  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;

  private Random random = new Random();

  // https://www.safaribooksonline.com/library/view/using-flume/9781491905326/ch04.html#channels
  // http://flume.apache.org/releases/content/1.5.0/apidocs/org/apache/flume/channel/file/FileChannel.html
  // NOTE: Setting FSYNC_PER_TXN to false allows skipping over corrupted events. See
  // https://github.com/apache/flume/blob/flume-1.5/flume-ng-channels/flume-file-channel/src/main/java/org/apache/flume/channel/file/FileChannel.java#L517

  private static ChannelThread channelThreads[];

  public static void init(Properties properties, KafkaProducer<byte[], byte[]> producer) throws IOException {
    String basePath = properties.getProperty("spool.path", "/tmp/kafka-rest");
    String checkpointPath = basePath + "/checkpoint";
    String backupCheckpointPath = basePath + "/backup_checkpoint";
    String dataPath = basePath + "/data";
    int partitions = Integer.parseInt(properties.getProperty("spool.partitions", "1"));
    log.info("Enabling " + partitions + " partitions for spooling under " + basePath);

    if (!new File(basePath).mkdirs()) {
      log.error("Cannot create spool path " + basePath);
      throw new IOException(basePath);
    }

    channelThreads = new ChannelThread[partitions];

    for (int partition = 0; partition < partitions; ++partition) {
      Context context = new Context();
      context.put(FileChannelConfiguration.CHECKPOINT_DIR, checkpointPath + "." + partition);
      context.put(FileChannelConfiguration.BACKUP_CHECKPOINT_DIR, backupCheckpointPath + "." + partition);
      context.put(FileChannelConfiguration.DATA_DIRS, dataPath + "." + partition);
      context.put(FileChannelConfiguration.FSYNC_PER_TXN, String.valueOf(false)); // skip corrupted events
      FileChannel channel = new FileChannel();
      channel.setName("spool#" + partition);
      Configurables.configure(channel, context);
      channel.start();
      channelThreads[partition] = new ChannelThread(channel, producer);
    }

    for (ChannelThread channelThread : channelThreads) {
      log.trace("Starting thread for " + channelThread.channel.getName());
      channelThread.start();
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override public void run() {
        for (ChannelThread channelThread : channelThreads) {
          log.trace("Signaling thread for " + channelThread.channel.getName());
          channelThread.interrupt();
        }
        for (ChannelThread channelThread : channelThreads) {
          try {
            channelThread.join();
            log.trace("Exited thread for " + channelThread.channel.getName());
          } catch (InterruptedException e) {
          }
        }
      }
    });
  }

  // The background thread that produces all spooled records asynchronously.
  static class ChannelThread extends Thread {
    public FileChannel channel;
    private KafkaProducer<byte[], byte[]> producer;

    public ChannelThread(FileChannel channel, KafkaProducer<byte[], byte[]> producer) {
      this.channel = channel;
      this.producer = producer;
    }

    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        ProducerRecord<byte[], byte[]> record = null;
        Callback callback = null;

        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
          Event attemptEvent = channel.take();
          final Event timestampEvent = channel.take();
          final Event topicEvent = channel.take();
          final Event partitionEvent = channel.take();
          final Event keyEvent = channel.take();
          final Event valueEvent = channel.take();

          if (attemptEvent != null &&
              timestampEvent != null &&
              topicEvent != null &&
              partitionEvent != null &&
              keyEvent != null &&
              valueEvent != null) {
            byte[] serializedAttempt = attemptEvent.getBody();
            byte[] serializedTimestamp = timestampEvent.getBody();
            byte[] serializedTopic = topicEvent.getBody();
            byte[] serializedPartition = partitionEvent.getBody();
            byte[] serializedKey = keyEvent.getBody();
            byte[] serializedValue = valueEvent.getBody();

            final Integer attempt = ByteBuffer.wrap(serializedAttempt).getInt();
            String topic = new String(serializedTopic);
            Integer partition = (serializedPartition.length) == 0 ? null :
              ByteBuffer.wrap(serializedPartition).getInt();

            record = new ProducerRecord<byte[], byte[]>(topic, partition, serializedKey, serializedValue);

            callback = new Callback() {
              public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e == null) {
                  log.trace("Produced record: " + metadata);
                  metrics.meter(MetricRegistry.name(SpoolProducer.class, "producer", "success")).mark();
                  // TODO: emit JMX gauge (System.currentTimeMillis() - serializedTimestamp) as spool_lag
                } else if (attempt < 9) { // TODO: make this configurable globally and per topic?
                  log.warn("Error producing record: " + e);
                  metrics.meter(MetricRegistry.name(SpoolProducer.class, "producer", "failure")).mark();
                  Transaction tr = channel.getTransaction();
                  tr.begin();
                  try {
                    // Re-enqueue the events and retry again later.
                    // TODO: incorporate some sort of exponential backoff
                    byte[] serializedAttempt = ByteBuffer.allocate(4).putInt(attempt + 1).array();
                    Event attemptEvent = EventBuilder.withBody(serializedAttempt);
                    channel.put(attemptEvent);
                    channel.put(timestampEvent);
                    channel.put(topicEvent);
                    channel.put(partitionEvent);
                    channel.put(keyEvent);
                    channel.put(valueEvent);
                    tr.commit();
                  } catch (ChannelException ex) {
                    log.error("Error spooling record: " + ex);
                    tr.rollback();
                  } finally {
                    tr.close();
                  }
                } else {
                  log.error("Dropping record: " + e);
                  metrics.meter(MetricRegistry.name(SpoolProducer.class, "producer", "dropped")).mark();
                }
              }
            };
            transaction.commit();
          } else {
            transaction.rollback();
            log.trace("No data");
            Thread.sleep(1000); // TODO: make this configurable?
          }
        } catch (ChannelException e) {
          log.error("Failed to read record: " + e);
          transaction.rollback();
          record = null;
          callback = null;
          // NOTE: What can be done at this point to mitigate the corrupted event? In the case if
          // FileChannelConfiguration.FSYNC_PER_TXN is true, then ChannelException will fire upon
          // corrupted event. What course of action can be taken at this point? If false, then
          // FileBackedTransaction.doTake will automatically skip over and find the next available
          // event.
        } catch (InterruptedException e) {
        } finally {
          transaction.close();
        }

        if (record != null && callback != null) {
          producer.send(record, callback);
        }
      }
    }
  }

  SpoolProducer(Map<String, Object> props, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    super(props, keySerializer, valueSerializer);

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  private void spool(ProducerRecord<K, V> record, long timestamp, Callback callback) {
    try {
      String recordTopic = record.topic();
      Integer recordPartition = record.partition();
      K recordKey = record.key();
      V recordValue = record.value();

      byte[] serializedAttempt = ByteBuffer.allocate(4).putInt(0).array();
      byte[] serializedTimestamp = ByteBuffer.allocate(8).putLong(timestamp).array();
      byte[] serializedTopic = recordTopic.getBytes();
      byte[] serializedPartition = (recordPartition == null) ? new byte[0] :
        ByteBuffer.allocate(4).putInt(recordPartition.intValue()).array();
      byte[] serializedKey = keySerializer.serialize(recordTopic, recordKey);
      byte[] serializedValue = valueSerializer.serialize(recordTopic, recordValue);

      Event attemptEvent = EventBuilder.withBody(serializedAttempt);
      Event timestampEvent = EventBuilder.withBody(serializedTimestamp);
      Event topicEvent = EventBuilder.withBody(serializedTopic);
      Event partitionEvent = EventBuilder.withBody(serializedPartition);
      Event keyEvent = EventBuilder.withBody(serializedKey);
      Event valueEvent = EventBuilder.withBody(serializedValue);

      int partition;
      if (recordPartition != null) {
        partition = recordPartition.hashCode() % channelThreads.length;
      } else if (serializedKey != null) {
        partition = serializedKey.hashCode() % channelThreads.length;
      } else {
        partition = random.nextInt(channelThreads.length);
      }
      FileChannel channel = channelThreads[partition].channel;
      Transaction transaction = channel.getTransaction();
      transaction.begin();
      try {
        channel.put(attemptEvent);
        channel.put(timestampEvent);
        channel.put(topicEvent);
        channel.put(partitionEvent);
        channel.put(keyEvent);
        channel.put(valueEvent);
        transaction.commit();

        TopicPartition tp = new TopicPartition(recordTopic, (recordPartition == null) ? -1 : recordPartition);
        RecordMetadata ack = new RecordMetadata(tp, -1, -1);
        log.trace("Spooled record: " + ack);
        metrics.meter(MetricRegistry.name(SpoolProducer.class, "producer", "spooled")).mark();
        // TODO: increase JMX counter spool_count
        callback.onCompletion(ack, null);
      } catch (Exception e) {
        transaction.rollback();
        throw e;
      } finally {
        transaction.close();
      }
    } catch (Exception e) {
      log.error("Failed to spool record: " + e);
      callback.onCompletion(null, e);
    }
  }

  void send(SpoolMode spoolMode, final ProducerRecord<K, V> record, final Callback callback) {
    final long timestamp = System.currentTimeMillis();

    if (spoolMode == SpoolMode.ASYNC) {
      spool(record, timestamp, callback);
    } else if (spoolMode == SpoolMode.SYNC) {
      Callback cb = new Callback() {
        public void onCompletion(RecordMetadata metadata, Exception e) {
          if (e == null) { // TODO: also catch certain types of exceptions and error immediately
            callback.onCompletion(metadata, null);
          } else {
            log.warn("Unable to produce record: " + e);
            spool(record, timestamp, callback);
          }
        }
      };
      send(record, cb);
    } else {
      send(record, callback);
    }
  }
}
