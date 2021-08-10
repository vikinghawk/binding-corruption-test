package com.cerner.test;

import com.rabbitmq.client.impl.recovery.BackoffPolicy;
import com.rabbitmq.client.impl.recovery.DefaultRetryHandler;
import com.rabbitmq.client.impl.recovery.RecordedEntity;
import com.rabbitmq.client.impl.recovery.TopologyRecoveryRetryLogic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomRetryHandler extends DefaultRetryHandler {

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
            .andThen(TopologyRecoveryRetryLogic.RECOVER_QUEUE)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_PREVIOUS_AUTO_DELETE_QUEUES),
        ctx -> null,
        TopologyRecoveryRetryLogic.RECOVER_CHANNEL
            .andThen(TopologyRecoveryRetryLogic.RECOVER_BINDING_QUEUE)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_BINDING)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_PREVIOUS_QUEUE_BINDINGS)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_PREVIOUS_AUTO_DELETE_QUEUES),
        TopologyRecoveryRetryLogic.RECOVER_CHANNEL
            .andThen(TopologyRecoveryRetryLogic.RECOVER_CONSUMER_QUEUE)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_CONSUMER)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_CONSUMER_QUEUE_BINDINGS)
            .andThen(TopologyRecoveryRetryLogic.RECOVER_PREVIOUS_CONSUMERS),
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
