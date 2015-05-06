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

import org.apache.flume.Transaction;
import org.apache.flume.channel.file.FileChannel;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

class SpoolProducer<K, V> extends KafkaProducer<K, V> {

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
  private static ArrayList<SpoolThread> spoolThreads;

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

      spoolThreads = new ArrayList<SpoolThread>();
      for (String spoolDir : spoolDirs.split(",")) {
        log.trace("Initializing thread for " + spoolDir.trim());
        SpoolThread thread = new SpoolThread(spoolDir.trim(), producer,
          consecutiveFailures, batchSize, batchTime, retryAttempts, retryBackoff);
        thread.start();
        spoolThreads.add(thread);
      }
    } catch (RestConfigException e) {
      log.error("Failed to initialize spool producer " + e);
    }
  }

  public static void shutdown() {
    for (SpoolThread spoolThread : spoolThreads) {
      log.trace("Signaling thread for " + spoolThread.queueChannel.getName());
      spoolThread.interrupt();
    }
    spoolThreads = null;
  }

  public SpoolProducer(Map<String, Object> props,
                       Serializer<K> keySerializer, Serializer<V> valueSerializer) {
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
          part = partition.hashCode() % spoolThreads.size();
        } else if (key != null) {
          part = key.hashCode() % spoolThreads.size();
        } else {
          part = (int)(timestamp % spoolThreads.size());
        }

        FileChannel channel = spoolThreads.get(part).queueChannel;
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
