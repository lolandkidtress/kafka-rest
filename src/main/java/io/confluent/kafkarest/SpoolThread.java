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

import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.conf.Configurables;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.confluent.kafkarest.entities.SpoolChannel;

import com.codahale.metrics.MetricRegistry;

// The thread that produces all spooled records asynchronously.
class SpoolThread extends Thread {

  private static final Logger log = LoggerFactory.getLogger(SpoolThread.class);

  private static final MetricRegistry metrics = new MetricRegistry();

  // Records are appended to the queue channel from the outward facing threads
  // and processed by this spool thread.
  public final FileChannel queueChannel;

  // Records are appended to the retry channel and later dequeued and enqueued
  // back into the queue channel by this spool thread. Records are repeatedly
  // tried until either successfully produced to the kafka cluster or if it gets
  // preserved in the error channel.
  private final FileChannel retryChannel;

  // Records are appended to the error channel by this spool thread and no
  // longer touched. Records can be revived by an external admin thread and
  // re-appended to the queue channel.
  public final FileChannel errorChannel;

  private final KafkaProducer<byte[], byte[]> producer;
  private final AtomicInteger consecutiveFailures;
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

  public SpoolThread(String basePath, KafkaProducer<byte[], byte[]> producer,
                     AtomicInteger consecutiveFailures,
                     int batchSize, int batchTime, int retryAttempts, int retryBackoff) {
    super(basePath);
    this.queueChannel = initChannel(new File(basePath, SpoolChannel.queue.toString()));
    this.retryChannel = initChannel(new File(basePath, SpoolChannel.retry.toString()));
    this.errorChannel = initChannel(new File(basePath, SpoolChannel.error.toString()));
    this.producer = producer;
    this.consecutiveFailures = consecutiveFailures;
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
          metrics.meter(MetricRegistry.name(SpoolThread.class, "producer", "success")).mark();
          consecutiveFailures.set(0);
          latch.countDown();
        } else {
          log.trace("Failed to produce record: " + metadata);
          metrics.meter(MetricRegistry.name(SpoolThread.class, "producer", "failure")).mark();
          consecutiveFailures.incrementAndGet();
          try {
            if (ex instanceof RetriableException && record.attempt % retryAttempts != 0) {
              log.warn("Cannot produce record: " + ex);
              try {
                preserve(retryChannel);
              } catch (Exception e) {
                log.error("Cannot spool to retry channel: " + e);
                // NOTE: It is not good to reach this point, but lets preserve
                // the record to the error channel so the spool thread can
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
            // spool thread will block on the irrecoverable error. The queue
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
        // updating the progress across the queue and error channels. If at any
        // point in the following for-loop an exception were to occur, the
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
                // The amount of time to sleep is arbitrary. We can make this a
                // configurable property, but a not well picked value can cause
                // significant delays. We might as well simply sleep until the
                // max batch time while capped at 1 second.
                Thread.sleep(Math.min(1000, ms));
              } catch (InterruptedException e) {
                // Delay the interruption and attempt to perform a clean thread
                // termination.
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
            // The spool thread has already been signaled to terminate. Lets
            // spend as much time as possible to drain the records or until the
            // main process terminates this thread.
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

        // NOTE: All records in this batch either have successfully reached the
        // kafka cluster or have been persisted into the retry channel. Commit
        // the queue transaction to reflect these messages have been completed.
        // If an unexpected failure occurs before the commit, the records in
        // this batch will be processed again by the subsequent iteration.
        // Duplicate records may end up in the kafka cluster, but no data loss
        // will occur.
        queueTransaction.commit();
      } catch (Exception e) {
        // Something has gone terribly wrong. Rollback and fail fast by bubbling
        // up the error to prevent processing any further records.
        log.error("Unexpected error: " + e);
        queueTransaction.rollback();
        terminate = true;
      } finally {
        queueTransaction.close();
      }

      if (!terminate && ts / retryBackoff > tsBucket) {
        // We entered the next timestamp bucket based on the retry interval and
        // need to move some records from the retry channel back to the queue
        // channel.
        tsBucket = ts / retryBackoff;
        // Revive records from the retry channel older than this time bucket.
        reviveRecords(retryChannel, tsBucket);
      }
    }

    // The spool thread is about to terminate. Clean up the channels.
    queueChannel.stop();
    retryChannel.stop();
    errorChannel.stop();

    log.trace("Stopped thread for " + queueChannel.getName());
  }
}
