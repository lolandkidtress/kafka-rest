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
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.kafkarest.entities.SpoolMode;
import io.confluent.kafkarest.exceptions.SpoolException;
import io.confluent.rest.RestConfigException;

import com.codahale.metrics.MetricRegistry;

// The goal of SpoolProducer is to provide a durable way to produce to the kafka
// cluster. SpoolProducer supports 3 different spool modes: DISABLED, SYNC, and
// ASYNC. With spool mode disabled (default), SpoolProducer behaves exactly like
// KafkaProducer, bubbling up all errors to the caller. With the synchronous
// mode, SpoolProducer falls back to spooling to local disk if the kafka cluster
// returns an error. In the asynchronous mode, records are spooled to local disk
// and immediately returns.

public class SpoolProducer<K, V> extends KafkaProducer<K, V> {

  // Each record is composed of the following FileChannel event fields:
  //   attempt: 4 byte integer
  //   timestamp: 8 byte integer representing the timestamp in milliseconds
  //   topic: variable length string representing the kafka topic name
  //   partition: 4 byte integer representing the partition # in the kafka topic
  //   key: byte array representing the key for the kafka message
  //   value: byte array representing the value for the kafka message

  // https://www.safaribooksonline.com/library/view/using-flume/9781491905326/ch04.html#channels
  // http://flume.apache.org/releases/content/1.5.0/apidocs/org/apache/flume/channel/file/FileChannel.html
  // NOTE: Setting FSYNC_PER_TXN to false allows skipping over corrupted events.
  // See https://github.com/apache/flume/blob/flume-1.5/flume-ng-channels/flume-file-channel/src/main/java/org/apache/flume/channel/file/FileChannel.java#L517

  private static final Logger log = LoggerFactory.getLogger(SpoolProducer.class);

  private static final MetricRegistry metrics = new MetricRegistry();

  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;

  // This is used as best effort to keep track of consecutive failures across
  // all producing threads. It should not be treated as an accurate reflection
  // of the true consecutive failure count. When any async producing thread sees
  // a failure, it increments this count. On success, it will reset this count
  // to zero. When the value exceeds the configured threshold, it will signal
  // send() to return an error immediately, regardless if the caller specified
  // to spool to disk.
  private static AtomicInteger consecutiveFailures = new AtomicInteger(0);

  private static int retryBackpressureThreshold;

  // Reference to a collection of threads used for asynchronously producing the
  // spooled messages. Each thread operates with its own set of queue, retry and
  // defunct files for better parallelism.
  private static ArrayList<ChannelThread> channelThreads;

  public static void init(Properties properties, KafkaProducer<byte[], byte[]> producer) {
    try {
      KafkaRestConfig config = new KafkaRestConfig(properties);
      // Comma separated directories
      String spoolDirs = config.getString(KafkaRestConfig.SPOOL_DIRS_CONFIG);
      // The number of retry attempts before placing into the defunct channel
      int retryAttempts = config.getInt(KafkaRestConfig.SPOOL_RETRY_ATTEMPTS_CONFIG);
      // The number of milliseconds between retry
      int retryBackoff = config.getInt(KafkaRestConfig.SPOOL_RETRY_BACKOFF_MS_CONFIG);
      // The number of consecutive failed attempts before erroring on spool
      retryBackpressureThreshold =
        config.getInt(KafkaRestConfig.SPOOL_RETRY_BACKPRESSURE_THRESHOLD_CONFIG);

      log.info("Retry up to " + (retryAttempts < 0 ? "unlimited" : retryAttempts) +
               " attempts with " + retryBackoff + "ms backoff interval");

      channelThreads = new ArrayList<ChannelThread>();
      for (String spoolDir : spoolDirs.split(",")) {
        log.trace("Initializing thread for " + spoolDir.trim());
        ChannelThread thread =
          new ChannelThread(spoolDir.trim(), producer, retryAttempts, retryBackoff);
        thread.start();
        channelThreads.add(thread);
      }
    } catch (RestConfigException e) {
      log.error("Failed to initialize spool producer " + e);
    }
  }

  public static void shutdown() {
    for (ChannelThread channelThread : channelThreads) {
      log.trace("Signaling thread for " + channelThread.queueChannel.getName());
      channelThread.interrupt();
    }
    channelThreads = null;
  }

  // The background thread that produces all spooled records asynchronously.
  static class ChannelThread extends Thread {

    // Records are appended to the queue channel from the outward facing threads
    // and processed by this background thread.
    public FileChannel queueChannel;

    // Records are appended to the retry channel and later processed by only
    // this background thread. Records are repeatedly re-appended until it is
    // either successfully produced to the kafka cluster or if it gets passed
    // over to the defunct channel.
    private FileChannel retryChannel;

    // Records are appended to the defunct channel by this background thread and
    // no longer touched. Records are revivied by an external admin thread and
    // re-appended to the queue channel.
    public FileChannel defunctChannel;

    private KafkaProducer<byte[], byte[]> producer;
    private int retryAttempts;
    private int retryBackoff;

    private FileChannel initChannel(File channelPath) {
      channelPath.mkdirs();
      Context context = new Context();
      context.put(FileChannelConfiguration.CHECKPOINT_DIR,
                  new File(channelPath, "checkpoint").getAbsolutePath());
      context.put(FileChannelConfiguration.BACKUP_CHECKPOINT_DIR,
                  new File(channelPath, "checkpoint.backup").getAbsolutePath());
      context.put(FileChannelConfiguration.DATA_DIRS,
                  new File(channelPath, "data").getAbsolutePath());
      context.put(FileChannelConfiguration.FSYNC_PER_TXN,
                  String.valueOf(false)); // configure to skip corrupted events
      FileChannel channel = new FileChannel();
      channel.setName(channelPath.getAbsolutePath());
      Configurables.configure(channel, context);
      channel.start();
      return channel;
    }

    public ChannelThread(String basePath, KafkaProducer<byte[], byte[]> producer,
                         int retryAttempts, int retryBackoff) {
      super(basePath);
      this.queueChannel = initChannel(new File(basePath, "queue"));
      this.retryChannel = initChannel(new File(basePath, "retry"));
      this.defunctChannel = initChannel(new File(basePath, "defunct"));
      this.producer = producer;
      this.retryAttempts = retryAttempts;
      this.retryBackoff = retryBackoff;
    }

    public void run() {
      log.trace("Started thread for " + queueChannel.getName());

      FileChannel channel = queueChannel;
      long tsBucket = System.currentTimeMillis() / retryBackoff;

      while (!Thread.currentThread().isInterrupted()) {
        ProducerRecord<byte[], byte[]> record = null;
        Callback callback = null;

        long ts = System.currentTimeMillis();
        long bucket = ts / retryBackoff;
        if (bucket > tsBucket) {
          // Switch over to drain the retry channel until it reaches the next record that does not
          // fall under the same retry timestamp bucket.
          channel = retryChannel;
          tsBucket = bucket;
        }

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

            // Determine if this record should still be retried
            final Integer attempt = ByteBuffer.wrap(serializedAttempt).getInt();
            Long timestamp = ByteBuffer.wrap(serializedTimestamp).getLong();
            boolean continueRetry = (ts - timestamp) >= (attempt * retryBackoff);

            if (channel == retryChannel && !continueRetry) {
              // Done draining current batch from retry channel. Switch back to processing records
              // from the normal queue channel.
              transaction.rollback();
              channel = queueChannel;
            } else {
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
                    consecutiveFailures.set(0);
                  } else {
                    log.warn("Error producing record: " + e);
                    metrics.meter(MetricRegistry.name(SpoolProducer.class, "producer", "failure")).mark();
                    consecutiveFailures.incrementAndGet();

                    byte[] serializedAttempt = ByteBuffer.allocate(4).putInt(attempt + 1).array();
                    Event attemptEvent = EventBuilder.withBody(serializedAttempt);

                    // Determine whether the record should be retried again. Using mod-operator so
                    // that records revived from the defunct channel can be retried again up to the
                    // specified attempts.
                    boolean shouldRetry = (retryAttempts != 0) &&
                                          ((attempt + 1) % retryAttempts == 0);
                    FileChannel ch = shouldRetry ? retryChannel : defunctChannel;
                    Transaction tr = ch.getTransaction();
                    tr.begin();
                    try {
                      // Enqueue the record to retry again later.
                      ch.put(attemptEvent);
                      ch.put(timestampEvent);
                      ch.put(topicEvent);
                      ch.put(partitionEvent);
                      ch.put(keyEvent);
                      ch.put(valueEvent);
                      tr.commit();
                    } catch (ChannelException ex) {
                      log.error("Error spooling record: " + ex);
                      tr.rollback();
                    } finally {
                      tr.close();
                    }
                  }
                }
              };
              transaction.commit();
            }
          } else {
            transaction.rollback();
            if (channel == queueChannel) {
              log.trace("No data");
              // Although with negligible overhead a mutex or semaphore can be used in conjunction
              // with keeping track of queue length to determine when exactly to wake up the thread,
              // it is hard to persist the length across process restart, especially after a crash.
              // Since the thread is handling async records, sleeping for a short period isn't that
              // bad. If sleeping for 250ms creates a backlog of records, then this condition will
              // not be met again for a while in the subsequent iterations.
              Thread.sleep(250);
              // NOTE: If additional logic gets added after Thread.sleep and requires cleaning up
              // the state, then add it under the InterruptedException catch-statement below.
            } else {
              // Switch back from the retry channel to the normal queue channel
              channel = queueChannel;
            }
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
          // event without throwing an exception.
        } catch (InterruptedException e) {
          // NOTE: There's nothing to do since this exception can only fire at the Thread.sleep
          // statement above. No state exists after that line requiring any clean up.
        } finally {
          transaction.close();
        }

        if (record != null && callback != null) {
          // Calling producer.send() out here because callback.onComplete() also tries to enter a
          // transaction, which may or may not be on the same thread as this background thread,
          // causing a potential deadlock.
          producer.send(record, callback);
        }
      }

      queueChannel.stop();
      retryChannel.stop();
      defunctChannel.stop();

      log.trace("Stopped thread for " + queueChannel.getName());
    }
  }

  SpoolProducer(Map<String, Object> props, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    super(props, keySerializer, valueSerializer);

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  private void spool(ProducerRecord<K, V> record, long timestamp, Callback callback) {
    int failures = consecutiveFailures.get();
    if (failures < retryBackpressureThreshold) {
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

        // Pick a partition to spool to
        int partition;
        if (recordPartition != null) {
          partition = recordPartition.hashCode() % channelThreads.size();
        } else if (serializedKey != null) {
          partition = serializedKey.hashCode() % channelThreads.size();
        } else {
          partition = (int)(timestamp % channelThreads.size());
        }

        FileChannel channel = channelThreads.get(partition).queueChannel;
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

          int part = (recordPartition == null) ? -1 : recordPartition;
          TopicPartition tp = new TopicPartition(recordTopic, part);
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
    } else {
      // Backpressure the error if too many consecutive failures
      callback.onCompletion(null, new SpoolException(failures));
    }
  }

  void send(final SpoolMode spoolMode, final ProducerRecord<K, V> record, final Callback callback) {
    // get timestamp when this record is first composed
    final long timestamp = System.currentTimeMillis();

    if (spoolMode == SpoolMode.ASYNC) {
      spool(record, timestamp, callback);
    } else if (spoolMode == SpoolMode.SYNC) {
      Callback cb = new Callback() {
        public void onCompletion(RecordMetadata metadata, Exception e) {
          // Treat other conditions such as brokers unavailable also as retriable
          if (e instanceof RetriableException) {
            log.warn("Unable to produce record: " + e);
            spool(record, timestamp, callback);
          } else {
            callback.onCompletion(metadata, e);
          }
        }
      };
      send(record, cb);
    } else {
      send(record, callback);
    }
  }
}
