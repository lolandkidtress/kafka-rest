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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
  // error files for better parallelism.
  private static ArrayList<ChannelThread> channelThreads;

  public static void init(Properties properties, KafkaProducer<byte[], byte[]> producer) {
    try {
      KafkaRestConfig config = new KafkaRestConfig(properties);
      // Comma separated directories
      String spoolDirs = config.getString(KafkaRestConfig.SPOOL_DIRS_CONFIG);
      // The number of records in a batch before producing to kafka
      int batchSize = config.getInt(KafkaRestConfig.SPOOL_BATCH_SIZE_CONFIG);
      // The number of milliseconds before cutting short the batch
      int batchTime = config.getInt(KafkaRestConfig.SPOOL_BATCH_MS_CONFIG);
      // The number of retry attempts before placing into the error channel
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
        ChannelThread thread = new ChannelThread(spoolDir.trim(), producer,
          batchSize, batchTime, retryAttempts, retryBackoff);
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

  // The thread that produces all spooled records asynchronously.
  static class ChannelThread extends Thread {

    // Records are appended to the queue channel from the outward facing threads
    // and processed by this channel thread.
    public final FileChannel queueChannel;

    // Records are appended to the retry channel and later dequeued and enqueued
    // back into the queue channel by this this channel thread. Records are
    // repeatedly tried until either successfully produced to the kafka cluster
    // or if it gets preserved in the error channel.
    private final FileChannel retryChannel;

    // Records are appended to the error channel by this channel thread and no
    // longer touched. Records can be revived by an external admin thread and
    // re-appended to the queue channel.
    public final FileChannel errorChannel;

    private final KafkaProducer<byte[], byte[]> producer;
    private final int batchSize;
    private final int batchTime;
    private final int retryAttempts;
    private final int retryBackoff;

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
                         int batchSize, int batchTime, int retryAttempts, int retryBackoff) {
      super(basePath);
      this.queueChannel = initChannel(new File(basePath, "queue"));
      this.retryChannel = initChannel(new File(basePath, "retry"));
      this.errorChannel = initChannel(new File(basePath, "error"));
      this.producer = producer;
      this.batchSize = batchSize;
      this.batchTime = batchTime;
      this.retryAttempts = retryAttempts;
      this.retryBackoff = retryBackoff;
    }

    // This is a helper method to construct the callback object for the spool
    // producer.
    private Callback getCallback(final CountDownLatch latch, final SpoolRecord record) {
      return new Callback() {
        private void preserve(FileChannel channel) throws Exception {
          Transaction transaction = channel.getTransaction();
          transaction.begin();
          try {
            // TODO: put record into channel
            record.put(channel);
            transaction.commit();
          } catch (Exception e) {
            log.warn("Cannot preserve record: " + e);
            transaction.rollback();
            throw e;
          } finally {
            transaction.close();
          }
        }
        public void onCompletion(RecordMetadata metadata, Exception ex) {
          if (ex == null) {
            log.trace("Produced record: " + metadata);
            metrics.meter(MetricRegistry.name(SpoolProducer.class, "producer", "success")).mark();
            consecutiveFailures.set(0);
            latch.countDown();
          } else {
            log.trace("Failed to produce record: " + metadata);
            metrics.meter(MetricRegistry.name(SpoolProducer.class, "producer", "failure")).mark();
            consecutiveFailures.incrementAndGet();
            try {
              if (ex instanceof RetriableException || record.attempt < retryAttempts) {
                log.warn("Cannot produce record: " + ex);
                try {
                  preserve(retryChannel);
                } catch (Exception e) {
                  log.error("Cannot spool to retry channel: " + e);
                  // NOTE: It is not good to reach this point, but lets preserve
                  // the record to the error channel so the channel thread can
                  // continue to process other records.
                  preserve(errorChannel);
                }
              } else {
                log.error("Cannot produce record: " + ex);
                // NOTE: Some non-retriable condition occured, lets preserve the
                // record to the error channel so the thread can continue to
                // process other records.
                preserve(errorChannel);
              }

              log.trace("Preserved record: " + metadata);
              latch.countDown();
            } catch (Exception e) {
              // Swallow exception. Because the latch is not decremented, this
              // channel thread will block on the irrecoverable error. The queue
              // transaction will not commit and therefore cause data loss.
              log.error("Cannot preserve record: " + e);
            }
          }
        }
      };
    }

    public void reviveRecords(FileChannel sourceChannel, long tsBucket) {
      boolean done = false;
      while (!done) {
        Transaction queueTransaction = queueChannel.getTransaction();
        Transaction sourceTransaction = sourceChannel.getTransaction();
        queueTransaction.begin();
        sourceTransaction.begin();
        try {
          for (int i = 0; i < batchSize && !done; ++i) {
            SpoolRecord record = SpoolRecord.take(sourceChannel);
            if (record != null && record.timestamp / retryBackoff <= tsBucket) {
              record.put(queueChannel);
            } else {
              done = true;
            }
          }
          queueTransaction.commit();
          sourceTransaction.commit();
        } catch (Exception e) {
          sourceTransaction.rollback();
          queueTransaction.rollback();
        } finally {
          sourceTransaction.close();
          queueTransaction.close();
        }
      }
    }

    public void run() {
      log.trace("Started thread for " + queueChannel.getName());

      // Process records from queue channel until this thread is signaled to
      // terminate.
      boolean terminate = false;
      long tsBucket = 0;
      while (!terminate && !Thread.interrupted()) {
        // Get the timestamp of this batch.
        long ts = System.currentTimeMillis();
        // Set up the expected number of records in this batch.
        CountDownLatch latch = new CountDownLatch(batchSize);

        Transaction queueTransaction = queueChannel.getTransaction();
        queueTransaction.begin();
        try {
          // NOTE: The following achieves a batching behavior while periodically
          // updating the progress across the queue and error channels. If at
          // any point in the following for-loop an exception were to occur, the
          // entire batch will rollback in the queue channel and retried by the
          // subsequent iteration.
          for (int i = 0; i < batchSize; ++i) {
            // The following takes a record from the queue channel and tries to
            // produce to the kafka cluster. The queue transaction is kept open
            // for the entire batch until the latch is fully drained by each of
            // the corresponding callbacks.
            SpoolRecord record = SpoolRecord.take(queueChannel);
            if (record != null) {
              // NOTE: Any unexpected failures from this point on will cause the
              // inflight records to potentially be produced "at least once".
              // However, it will guarantee no data loss.
              // Send the record via the async producer to the kafka cluster.
              producer.send(record.payload, getCallback(latch, record));

              // If this batch takes too long to fill, cut it short and continue
              // processing in the next batch. This is for preventing the queue
              // transaction from being kept open too long due to inactivity.
              if (System.currentTimeMillis() - ts >= batchTime) {
                // The following will also indirectly terminate the for-loop.
                while (++i < batchSize) {
                  latch.countDown();
                }
              }
            } else {
              // No more records currently in the queue channel. Sleep for a
              // little while and continue as new batch.
              long ms = batchTime - (System.currentTimeMillis() - ts);
              if (ms > 0) {
                try {
                  // The amount of time to sleep is arbitrary. We can make this
                  // a configurable property, but a not well picked value can
                  // cause significant delays. We might as well simply sleep
                  // until the max batch time while capped at 1 second.
                  Thread.sleep(Math.min(1000, ms));
                } catch (InterruptedException e) {
                  // Delay the interruption and attempt to perform a clean
                  // thread termination.
                  terminate = true;
                }
              }

              // Lets start a new batch so we can avoid the queue transaction
              // being opened too long. The following will also indirecty
              // terminate the for-loop.
              while (i++ < batchSize) {
                latch.countDown();
              }
            }
          }

          // The batch of records are underway. Wait for the async producer to
          // acknowledge all inflight records have either been successfully
          // produced to the kafka cluster, or inserted into the retry channel
          // upon failures.
          try {
            if (terminate) {
              // The channel thread has already been signaled to terminate. Lets
              // spend as much time as possible to drain the records or until
              // the main process terminates this thread.
              latch.await();
            } else if (!latch.await(Math.max(1000L, batchTime * 100), TimeUnit.MILLISECONDS)) {
              // The amount of time to block before deciding to terminate the
              // thread due to some very bad condition is arbitrary. Picking 100
              // times the max batch time sounds reasonably high, yet won't be
              // forever.
              throw new Exception("Stuck waiting for producers");
            }
          } catch (InterruptedException e) {
            // Delay the interruption and attempt to continue the clean thread
            // termination logic.
            terminate = true;
          }

          // NOTE: All records in this batch either have successfully reached
          // the kafka cluster or have been persisted into the retry channel.
          // Commit the queue transaction to acknowledge these messages have
          // been completed. If an unexpected failure occurs before the commit,
          // the records in this batch will be processed again by the subsequent
          // iteration. Duplicate records may end up in the kafka cluster, but
          // no data loss will occur.
          queueTransaction.commit();
        } catch (Exception e) {
          // Something has gone terribly wrong. Rollback and fail fast by
          // bubbling up the error to prevent processing any further records.
          log.error("Unexpected error: " + e);
          queueTransaction.rollback();
          terminate = true;
        } finally {
          queueTransaction.close();
        }

        if (!terminate && ts / retryBackoff > tsBucket) {
          // We entered the next timestamp bucket based on the retry interval
          // and need to move some records from the retry channel back to the
          // queue channel.
          tsBucket = ts / retryBackoff;
          // Revive records from the retry channel older than this time bucket.
          reviveRecords(retryChannel, tsBucket);
        }
      }

      // The channel thread is about to terminate. Clean up the channels.
      queueChannel.stop();
      retryChannel.stop();
      errorChannel.stop();

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
        String topic = record.topic();
        Integer partition = record.partition();
        byte[] key = keySerializer.serialize(topic, record.key());
        byte[] value = valueSerializer.serialize(topic, record.value());
        SpoolRecord rec = new SpoolRecord(timestamp, topic, partition, key, value);

        // Pick a partition to spool to
        int part;
        if (partition != null) {
          part = partition.hashCode() % channelThreads.size();
        } else if (key != null) {
          part = key.hashCode() % channelThreads.size();
        } else {
          part = (int)(timestamp % channelThreads.size());
        }

        FileChannel channel = channelThreads.get(part).queueChannel;
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
          rec.put(channel);
          transaction.commit();

          TopicPartition tp = new TopicPartition(topic, (partition == null) ? -1 : partition);
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
    this.attempt = ByteBuffer.wrap(attemptEvent.getBody()).getInt();
    this.timestamp = ByteBuffer.wrap(timestampEvent.getBody()).getLong();
    byte[] partition = partitionEvent.getBody();
    this.payload = new ProducerRecord<byte[], byte[]>(new String(topicEvent.getBody()),
      (partition.length) == 0 ? null : ByteBuffer.wrap(partition).getInt(),
      keyEvent.getBody(), valueEvent.getBody());
    this.attemptEvent = EventBuilder.withBody(ByteBuffer.allocate(4).putInt(attempt + 1).array());
    this.timestampEvent = timestampEvent;
    this.topicEvent = topicEvent;
    this.partitionEvent = partitionEvent;
    this.keyEvent = keyEvent;
    this.valueEvent = valueEvent;
  }

  // Construct a new spool record.
  public SpoolRecord(long timestamp, String topic, Integer partition, byte[] key, byte[] value) {
    this.attempt = 0;
    this.timestamp = timestamp;
    this.payload = new ProducerRecord<byte[], byte[]>(topic, partition, key, value);
    byte[] serializedAttempt = ByteBuffer.allocate(4).putInt(0).array();
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
