package com.cerner.test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSocketConfigurator;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryDelayHandler.ExponentialBackoffDelayHandler;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.TopologyRecoveryException;
import com.rabbitmq.client.impl.DefaultCredentialsProvider;
import com.rabbitmq.client.impl.ForgivingExceptionHandler;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.impl.recovery.BackoffPolicy;
import com.rabbitmq.client.impl.recovery.DefaultRetryHandler;
import com.rabbitmq.client.impl.recovery.RecordedBinding;
import com.rabbitmq.client.impl.recovery.RecordedConsumer;
import com.rabbitmq.client.impl.recovery.RecordedEntity;
import com.rabbitmq.client.impl.recovery.RecordedExchange;
import com.rabbitmq.client.impl.recovery.RecordedQueue;
import com.rabbitmq.client.impl.recovery.RecordedQueueBinding;
import com.rabbitmq.client.impl.recovery.RetryContext;
import com.rabbitmq.client.impl.recovery.TopologyRecoveryFilter;
import com.rabbitmq.client.impl.recovery.TopologyRecoveryRetryLogic;
import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.ClientParameters;
import com.rabbitmq.http.client.HttpComponentsRestTemplateConfigurator;
import com.rabbitmq.utility.Utility;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.core.env.Environment;

public class Utils {

  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  private Utils() {}

  public static Client createMgmtClient(
      final RabbitProperties rabbitProps, final BindingCorruptionDetectorProperties detectorProps)
      throws MalformedURLException, URISyntaxException {
    final ClientParameters params =
        new ClientParameters()
            .restTemplateConfigurator(new HttpComponentsRestTemplateConfigurator());
    params.url(detectorProps.getHttpApiUri());
    // user & pass might have already been set as part of rabbit-http-api-uri prop
    if (params.getUsername() == null) {
      params.username(rabbitProps.determineUsername());
    }
    if (params.getPassword() == null) {
      params.password(rabbitProps.determinePassword());
    }
    log.info("Connecting httpclient to {}", params.getUrl());
    return new Client(params);
  }

  public static void initTasConfig(
      final RabbitProperties rabbitProps,
      final Environment environment,
      final BindingCorruptionTestProperties testProps,
      final BindingCorruptionDetectorProperties detectorProps) {
    if ((rabbitProps.getHost() == null || rabbitProps.getHost().equalsIgnoreCase("localhost"))
        && testProps.getRabbitServiceName() != null) {
      final String propPrefix = "vcap.services." + testProps.getRabbitServiceName();
      final String rabbitClusterUris = environment.getProperty(propPrefix + ".credentials.uris");
      if (rabbitClusterUris == null) {
        return;
      }
      rabbitProps.setAddresses(rabbitClusterUris);
      if (detectorProps != null) {
        detectorProps.setHttpApiUri(
            environment.getProperty(propPrefix + ".credentials.http_api_uri"));
      }
    }
  }

  public static AutorecoveringConnection createConnection(
      final RabbitProperties props,
      final BindingCorruptionTestProperties customProps,
      final String name)
      throws Exception {
    log.info("Connecting to {} for connection={}", props.determineAddresses(), name);
    final ConnectionFactory cf = getConnectionFactory(props, customProps, name);
    final AutorecoveringConnection connection =
        (AutorecoveringConnection)
            cf.newConnection(
                Executors.newCachedThreadPool(),
                Address.parseAddresses(props.determineAddresses()),
                name);
    connection.addRecoveryListener(
        new RetryingRecoveryListener(
            connection, name, (RetryingExceptionHandler) cf.getExceptionHandler(), customProps));
    connection.addShutdownListener(
        cause -> {
          if (cause.isInitiatedByApplication()) {
            return;
          }
          if (cause.isHardError()) {
            log.error(
                "Error occurred on connection={} due to reason={}", name, cause.getReason(), cause);
          } else {
            log.error(
                "Error occurred on a channel for connection={} due to reason={}",
                name,
                cause.getReason(),
                cause);
          }
        });
    log.info("Connected to {} for connection={}", connection.getAddress(), name);
    return connection;
  }

  private static ConnectionFactory getConnectionFactory(
      final RabbitProperties props,
      final BindingCorruptionTestProperties customProps,
      final String name)
      throws KeyManagementException, NoSuchAlgorithmException {
    final ConnectionFactory cf = new ConnectionFactory();
    if (props.getSsl().determineEnabled()) {
      cf.useSslProtocol();
      cf.enableHostnameVerification();
    }
    cf.setHost(props.determineHost());
    cf.setPort(props.determinePort());
    cf.setVirtualHost(props.determineVirtualHost());
    cf.setCredentialsProvider(
        new DefaultCredentialsProvider(props.determineUsername(), props.determinePassword()));
    // enable recovery
    cf.setAutomaticRecoveryEnabled(true);
    cf.setTopologyRecoveryEnabled(true);
    cf.setTopologyRecoveryFilter(
        new TopologyRecoveryFilter() {
          @Override
          public boolean filterExchange(final RecordedExchange recordedExchange) {
            // ignore our topic exchange. It is durable & doesn't need recovered
            // trying to recover one can cause issues if the channel that declared it has been
            // closed
            return !recordedExchange.getName().equalsIgnoreCase(customProps.getTopicExchange());
          }

          @Override
          public boolean filterConsumer(final RecordedConsumer recordedConsumer) {
            return !"amq.rabbitmq.reply-to".equals(recordedConsumer.getQueue());
          }
        });
    List<Long> delays = customProps.getRecoveryDelays();
    if (delays.isEmpty()) {
      // Note: On a cluster with no load, i can't recreate the binding corruption issue with higher
      // delays here such as 5 seconds. But on a cluster with message load, i have tested setting
      // this as high as 10 seconds and still see binding corruptions.

      //      delays = Arrays.asList(1L, 1000L, 1000L, 2000L, 3000L, 5000L, 8000L, 13000L);
      delays = Arrays.asList(1000L);
      //      delays = Arrays.asList(5000L);
    }
    cf.setRecoveryDelayHandler(new ExponentialBackoffDelayHandler(delays));
    cf.setTopologyRecoveryRetryHandler(
        new CustomRetryHandler(
            customProps.getMaxTopologyRecoveryRetries(),
            nbAttempts -> Thread.sleep(Math.min(nbAttempts * 1000L, 5000L)),
            name));
    // configure timeouts
    cf.setRequestedHeartbeat(30);
    cf.setShutdownTimeout(5000);
    cf.setChannelRpcTimeout(300000);
    cf.setConnectionTimeout(60000);
    cf.useBlockingIo();
    cf.setSocketConfigurator(
        new DefaultSocketConfigurator() {
          @Override
          public void configure(final Socket socket) throws IOException {
            super.configure(socket);
            socket.setSoTimeout(15000);
          }
        });
    cf.setChannelShouldCheckRpcResponseType(true);
    cf.setExceptionHandler(new RetryingExceptionHandler(name));
    return cf;
  }

  /* Same as TopologyRecoveryRetryLogic.RETRY_ON_QUEUE_NOT_FOUND_RETRY_HANDLER but with modified logging */
  static class CustomRetryHandler extends DefaultRetryHandler {

    // TODO Submit PR back to amqp-client
    public static final DefaultRetryHandler.RetryOperation<Void>
        RECOVER_PREVIOUS_AUTO_DELETE_QUEUES =
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
                for (final RecordedBinding binding :
                    Utility.copy(connection.getRecordedBindings())) {
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
        final RecordedConsumer consumer, final AutorecoveringConnection connection)
        throws Exception {
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

  static class RetryingRecoveryListener implements RecoveryListener {

    private final AutorecoveringConnection connection;
    private final String name;
    private final RetryingExceptionHandler exceptionHandler;
    private final BindingCorruptionTestProperties props;
    private int recoveryRetries = 0;
    private volatile long lastConnectionLoss;

    RetryingRecoveryListener(
        final AutorecoveringConnection connection,
        final String name,
        final RetryingExceptionHandler exceptionHandler,
        final BindingCorruptionTestProperties props) {
      this.connection = connection;
      this.name = name;
      this.exceptionHandler = exceptionHandler;
      this.props = props;
    }

    @Override
    public void handleRecoveryStarted(final Recoverable recoverable) {
      lastConnectionLoss = System.nanoTime();
      log.info("Recovery started for connection={}", name);
    }

    @Override
    public void handleTopologyRecoveryStarted(final Recoverable recoverable) {
      log.info("Reconnected connection={}, starting topology recovery.", name);
    }

    @Override
    public void handleRecovery(final Recoverable recoverable) {
      final int errorCount = exceptionHandler.topologyRecoveryErrors.getAndSet(0);
      if (errorCount > 0) {
        if (recoveryRetries++ <= props.getMaxConnectionResetRecoveryRetries()) {
          log.warn(
              "Finished recovery for connection={} with {} recovery errors! Force closing connection in {} seconds to retry recovery again. Attempt {} of {}",
              name,
              errorCount,
              recoveryRetries,
              recoveryRetries,
              props.getMaxConnectionResetRecoveryRetries());
          forceClose(connection, recoveryRetries, lastConnectionLoss);
        } else {
          log.error(
              "WARNING! Finished recovery for connection={} with {} recovery errors! Reached max of {} recovery retry attempts. Continuing on with failed recovery.",
              name,
              errorCount,
              props.getMaxConnectionResetRecoveryRetries());
          recoveryRetries = 0;
        }
      } else {
        recoveryRetries = 0;
        log.info("Finished recovery for connection={} with no recovery errors", name);
      }
    }

    void forceClose(
        final AutorecoveringConnection connection,
        final long waitSeconds,
        final long expectedLastConnectionLoss) {
      final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
      exec.schedule(
          () -> {
            try {
              if (!connection.isOpen() || expectedLastConnectionLoss != lastConnectionLoss) {
                log.info("Not force closing connection={} as it was already closed", name);
              } else {
                connection
                    .getDelegate()
                    .close(
                        AMQP.CONNECTION_FORCED,
                        "Force closing connection to retry failed recovery",
                        false,
                        new Exception("Force closing connection to retry failed recovery"));
              }
            } catch (final Exception e) {
              log.error("Error force closing connection={} to retry failed recovery", name, e);
            } finally {
              exec.shutdown();
            }
          },
          waitSeconds,
          TimeUnit.SECONDS);
    }
  }

  static class RetryingExceptionHandler extends ForgivingExceptionHandler {

    final String name;
    final AtomicInteger topologyRecoveryErrors = new AtomicInteger();

    RetryingExceptionHandler(final String name) {
      this.name = name;
    }

    @Override
    public void handleTopologyRecoveryException(
        final Connection conn, final Channel ch, final TopologyRecoveryException exception) {
      topologyRecoveryErrors.incrementAndGet();
      super.handleTopologyRecoveryException(conn, ch, exception);
    }

    @Override
    protected void log(final String message, final Throwable e) {
      if (isSocketClosedOrConnectionReset(e)) {
        log.warn(
            "Exception on connection={}. {} (Exception message: {}", name, message, e.getMessage());
      } else {
        log.error("Exception on connection={}. {}", name, message, e);
      }
    }

    private static boolean isSocketClosedOrConnectionReset(final Throwable e) {
      return e instanceof IOException
          && ("Connection reset".equals(e.getMessage())
              || "Socket closed".equals(e.getMessage())
              || "Connection reset by peer".equals(e.getMessage()));
    }
  }
}
