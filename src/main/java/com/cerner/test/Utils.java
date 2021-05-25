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
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.impl.recovery.RecordedConsumer;
import com.rabbitmq.client.impl.recovery.RecordedExchange;
import com.rabbitmq.client.impl.recovery.TopologyRecoveryFilter;
import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.ClientParameters;
import com.rabbitmq.http.client.HttpComponentsRestTemplateConfigurator;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
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
      delays = Arrays.asList(1L, 1000L, 1000L, 2000L, 3000L, 5000L, 8000L, 13000L);
    }
    cf.setRecoveryDelayHandler(new ExponentialBackoffDelayHandler(delays));
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

  static class RetryingRecoveryListener implements RecoveryListener {

    private final AutorecoveringConnection connection;
    private final String name;
    private final RetryingExceptionHandler exceptionHandler;
    private final BindingCorruptionTestProperties props;
    private int recoveryRetries = 0;

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
        if (recoveryRetries++ <= props.getMaxTopologyRecoveryRetries()) {
          log.warn(
              "Finished recovery for connection={} with {} recovery errors! Force closing connection in {} seconds to retry recovery again. Attempt {} of {}",
              name,
              errorCount,
              recoveryRetries,
              recoveryRetries,
              props.getMaxTopologyRecoveryRetries());
          forceClose(connection, recoveryRetries);
        } else {
          log.warn(
              "Finished recovery for connection={} with {} recovery errors! Reached max recovery retry attempts. Continuing on with failed recovery.",
              name,
              errorCount);
          recoveryRetries = 0;
        }
      } else {
        recoveryRetries = 0;
        log.info("Finished recovery for connection={} with no recovery errors", name);
      }
    }

    void forceClose(final AutorecoveringConnection connection, final long waitSeconds) {
      final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
      exec.schedule(
          () -> {
            try {
              if (connection.isOpen()) {
                connection
                    .getDelegate()
                    .close(
                        AMQP.CONNECTION_FORCED,
                        "Force closing connection to retry failed recovery",
                        false,
                        new Exception("Force closing connection to retry failed recovery"));
              }
              return null;
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
