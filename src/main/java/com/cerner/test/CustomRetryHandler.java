package com.cerner.test;

import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.impl.recovery.BackoffPolicy;
import com.rabbitmq.client.impl.recovery.DefaultRetryHandler;
import com.rabbitmq.client.impl.recovery.RecordedBinding;
import com.rabbitmq.client.impl.recovery.RecordedConsumer;
import com.rabbitmq.client.impl.recovery.RecordedEntity;
import com.rabbitmq.client.impl.recovery.RecordedQueue;
import com.rabbitmq.client.impl.recovery.RecordedQueueBinding;
import com.rabbitmq.client.impl.recovery.RetryContext;
import com.rabbitmq.client.impl.recovery.TopologyRecoveryRetryLogic;
import com.rabbitmq.utility.Utility;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fixes issues with TopologyRecoveryRetryLogic.RETRY_ON_QUEUE_NOT_FOUND_RETRY_HANDLER where
 * auto-delete queues are not getting recreated on retry
 */
public class CustomRetryHandler extends DefaultRetryHandler {

  // TODO Submit PR back to amqp-client
  public static final DefaultRetryHandler.RetryOperation<Void> RECOVER_PREVIOUS_AUTO_DELETE_QUEUES =
      context -> {
        if (context.entity() instanceof RecordedQueue) {
          AutorecoveringConnection connection = context.connection();
          RecordedQueue queue = context.queue();
          // recover all queues for the same channel that had already been recovered
          // successfully before this queue failed. If the previous ones were auto-delete or
          // exclusive, they need recovered again
          for (Entry<String, RecordedQueue> entry :
              Utility.copy(connection.getRecordedQueues()).entrySet()) {
            if (entry.getValue() == queue) {
              // we have gotten to the queue in this context. Since this is an ordered map we
              // can now break as we know we have recovered all the earlier queues on this
              // channel
              break;
            } else if (queue.getChannel() == entry.getValue().getChannel()
                && entry.getValue().isAutoDelete()) {
              // TODO need to expose RecordedQueue.isExclusive() to be used here too
              connection.recoverQueue(entry.getKey(), entry.getValue(), false);
            }
          }
        } else if (context.entity() instanceof RecordedQueueBinding) {
          AutorecoveringConnection connection = context.connection();
          Set<String> queues = new LinkedHashSet<>();
          for (Entry<String, RecordedQueue> entry :
              Utility.copy(connection.getRecordedQueues()).entrySet()) {
            if (context.entity().getChannel() == entry.getValue().getChannel()
                && entry.getValue().isAutoDelete()) { // TODO isExclusive here too
              connection.recoverQueue(entry.getKey(), entry.getValue(), false);
              queues.add(entry.getValue().getName());
            }
          }
          for (final RecordedBinding binding : Utility.copy(connection.getRecordedBindings())) {
            if (binding instanceof RecordedQueueBinding
                && queues.contains(binding.getDestination())) {
              binding.recover();
            }
          }
        }
        return null;
      };

  // TODO All 4 of these next ones fix logic in TopologyRecoveryRetryLogic
  // Need to call recordedqueue.recover() instead of connection.recoverQueue()
  // connection.recoverQueue swallows any additional failures and we don't keep retrying
  // On PR, still need a way to update queue name for server named queues

  public static final DefaultRetryHandler.RetryOperation<Void> RECOVER_QUEUE =
      context -> {
        if (context.entity() instanceof RecordedQueue) {
          final RecordedQueue recordedQueue = context.queue();
          // connection.recoverQueue(recordedQueue.getName(), recordedQueue, false);
          recordedQueue.recover();
        }
        return null;
      };

  public static final DefaultRetryHandler.RetryOperation<Void> RECOVER_BINDING_QUEUE =
      context -> {
        if (context.entity() instanceof RecordedQueueBinding) {
          RecordedBinding binding = context.binding();
          AutorecoveringConnection connection = context.connection();
          RecordedQueue recordedQueue =
              connection.getRecordedQueues().get(binding.getDestination());
          if (recordedQueue != null) {
            // connection.recoverQueue(recordedQueue.getName(), recordedQueue, false);
            recordedQueue.recover();
          }
        }
        return null;
      };

  public static final DefaultRetryHandler.RetryOperation<Void> RECOVER_CONSUMER_QUEUE =
      context -> {
        if (context.entity() instanceof RecordedConsumer) {
          RecordedConsumer consumer = context.consumer();
          AutorecoveringConnection connection = context.connection();
          RecordedQueue recordedQueue = connection.getRecordedQueues().get(consumer.getQueue());
          if (recordedQueue != null) {
            // connection.recoverQueue(recordedQueue.getName(), recordedQueue, false);
            recordedQueue.recover();
          }
        }
        return null;
      };

  public static final DefaultRetryHandler.RetryOperation<String> RECOVER_PREVIOUS_CONSUMERS =
      context -> {
        if (context.entity() instanceof RecordedConsumer) {
          final AutorecoveringChannel channel = context.consumer().getChannel();
          for (RecordedConsumer consumer :
              Utility.copy(context.connection().getRecordedConsumers()).values()) {
            if (consumer == context.entity()) {
              break;
            } else if (consumer.getChannel() == channel) {
              final RetryContext retryContext =
                  new RetryContext(consumer, context.exception(), context.connection());
              RECOVER_CONSUMER_QUEUE.call(retryContext);
              // context.connection().recoverConsumer(consumer.getConsumerTag(), consumer, false);
              recoverConsumer(consumer, context.connection());
              TopologyRecoveryRetryLogic.RECOVER_CONSUMER_QUEUE_BINDINGS.call(retryContext);
            }
          }
          return context.consumer().getConsumerTag();
        }
        return null;
      };

  private static void recoverConsumer(
      final RecordedConsumer consumer, final AutorecoveringConnection connection) throws Exception {
    final String tag = consumer.getConsumerTag();
    final String newTag = consumer.recover();
    if (tag != null && !tag.equals(newTag)) {
      final Map<String, RecordedConsumer> consumers = connection.getRecordedConsumers();
      synchronized (consumers) {
        consumers.remove(tag);
        consumers.put(newTag, consumer);
      }
      final Method m =
          AutorecoveringChannel.class.getDeclaredMethod(
              "updateConsumerTag", String.class, String.class);
      m.setAccessible(true);
      m.invoke(consumer.getChannel(), tag, newTag);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(CustomRetryHandler.class);

  private final String connectionName;

  public CustomRetryHandler(
      final int retryAttempts, final BackoffPolicy backoffPolicy, final String connectionName) {
    super(
        TopologyRecoveryRetryLogic.CHANNEL_CLOSED_NOT_FOUND,
        (q, e) -> false,
        TopologyRecoveryRetryLogic.CHANNEL_CLOSED_NOT_FOUND,
        TopologyRecoveryRetryLogic.CHANNEL_CLOSED_NOT_FOUND,
        TopologyRecoveryRetryLogic.RECOVER_CHANNEL
            .andThen(RECOVER_QUEUE)
            .andThen(RECOVER_PREVIOUS_AUTO_DELETE_QUEUES),
        ctx -> null,
        TopologyRecoveryRetryLogic.RECOVER_CHANNEL
            .andThen(RECOVER_BINDING_QUEUE)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_BINDING)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_PREVIOUS_QUEUE_BINDINGS)
            .andThen(RECOVER_PREVIOUS_AUTO_DELETE_QUEUES),
        TopologyRecoveryRetryLogic.RECOVER_CHANNEL
            .andThen(RECOVER_CONSUMER_QUEUE)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_CONSUMER)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_CONSUMER_QUEUE_BINDINGS)
            .andThen(RECOVER_PREVIOUS_CONSUMERS),
        retryAttempts,
        backoffPolicy);
    this.connectionName = connectionName;
  }

  @Override
  protected void log(final RecordedEntity entity, final Exception exception, final int attempts) {
    log.info(
        "Error while recovering {} for connection={}, retrying with {} more attempt(s).",
        entity,
        connectionName,
        retryAttempts - attempts,
        exception);
  }
}
