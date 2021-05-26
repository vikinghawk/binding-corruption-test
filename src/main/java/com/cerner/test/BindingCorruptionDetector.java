package com.cerner.test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.BindingInfo;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

public class BindingCorruptionDetector implements CommandLineRunner, EnvironmentAware {

  private static final Logger log = LoggerFactory.getLogger(BindingCorruptionDetector.class);

  private final RabbitProperties rabbitProps;
  private final BindingCorruptionDetectorProperties detectorProps;
  private final BindingCorruptionTestProperties testProps;
  private Environment environment;

  public BindingCorruptionDetector(
      final RabbitProperties rabbitProperties,
      final BindingCorruptionDetectorProperties detectorProps,
      final BindingCorruptionTestProperties testProps) {
    this.rabbitProps = rabbitProperties;
    this.detectorProps = detectorProps;
    this.testProps = testProps;
  }

  @Override
  public void setEnvironment(final Environment environment) {
    this.environment = environment;
  }

  @Override
  public void run(final String... args) throws Exception {
    Utils.initTasConfig(rabbitProps, environment, testProps, detectorProps);
    try (final AutorecoveringConnection connection =
        Utils.createConnection(rabbitProps, testProps, "BindingCorruptionDetector")) {
      final Client client = Utils.createMgmtClient(rabbitProps, detectorProps);
      detectBindingCorruption(connection, client, false);
    }
  }

  private void detectBindingCorruption(
      final AutorecoveringConnection connection,
      final Client client,
      final boolean isVerifyingAfterRepair)
      throws Exception {
    // get all bindings
    log.info("Finding all topic bindings");
    final Set<String> routingKeys =
        client.getBindingsBySource(rabbitProps.determineVirtualHost(), testProps.getTopicExchange())
            .stream()
            .map(BindingInfo::getRoutingKey)
            .collect(Collectors.toSet());

    final List<Return> returns = Collections.synchronizedList(new ArrayList<>());
    try (final Channel channel = connection.createChannel()) {
      channel.addReturnListener(
          r -> {
            returns.add(r);
            log.info(
                "Received return on exchange={}, routingKey={}",
                r.getExchange(),
                r.getRoutingKey());
          });

      log.info("Publishing mandatory messages to {} different routingKeys", routingKeys.size());
      for (final String routingKey : routingKeys) {
        final BasicProperties.Builder props = new BasicProperties.Builder();
        props.messageId(UUID.randomUUID().toString());
        props.expiration(String.valueOf(detectorProps.getWaitMillis() + 5000));
        final byte[] body =
            String.valueOf("BindingCorruptionDetector for routingKey=" + routingKey)
                .getBytes(StandardCharsets.UTF_8);
        channel.basicPublish(testProps.getTopicExchange(), routingKey, true, props.build(), body);
      }
      log.info(
          "Finished publishing messages, waiting {} ms to receive un-routed messages",
          detectorProps.getWaitMillis());
      Thread.sleep(detectorProps.getWaitMillis());
    }
    // now log the results
    if (returns.isEmpty()) {
      log.info(
          "BindingCorruptionDetector was successful! All {} messages were routed to atleast one queue.",
          routingKeys.size());
      return;
    }
    if (isVerifyingAfterRepair) {
      log.error(
          "WARNING! BindingCorruptionDetector failed even after repairing. The following {} routingKeys were not routed to any queues: {}",
          returns.size(),
          returns.stream().map(Return::getRoutingKey).collect(Collectors.toList()));
    } else {
      log.warn(
          "BindingCorruptionDetector failed. The following {} routingKeys were not routed to any queues: {}",
          returns.size(),
          returns.stream().map(Return::getRoutingKey).collect(Collectors.toList()));
      // now fix the bindings!
      if (detectorProps.isRepairCorruptBindings()) {
        // repair any exchange bindings and find the list of queues to repair bindings for
        final Set<String> routingKeysToFix =
            returns.stream().map(Return::getRoutingKey).collect(Collectors.toSet());
        client.getBindingsBySource(rabbitProps.determineVirtualHost(), testProps.getTopicExchange())
            .stream()
            .filter(binding -> routingKeysToFix.contains(binding.getRoutingKey()))
            .forEach(binding -> repairBinding(client, binding));
        log.info(
            "Finished repairing bindings. Re-running BindingCorruptionDetector to verify bindings were fixed.");
        detectBindingCorruption(connection, client, true);
      }
    }
  }

  private static void repairBinding(final Client client, final BindingInfo binding) {
    try {
      log.info(
          "Repairing queue binding between {} and {} with routingKey={}",
          binding.getSource(),
          binding.getDestination(),
          binding.getRoutingKey());
      client.unbindQueue(
          binding.getVhost(),
          binding.getDestination(),
          binding.getSource(),
          binding.getRoutingKey());
      client.bindQueue(
          binding.getVhost(),
          binding.getDestination(),
          binding.getSource(),
          binding.getRoutingKey(),
          binding.getArguments());
    } catch (final Exception e) {
      log.error(
          "Error repairing binding on between {} and {} with routingKey={}",
          binding.getSource(),
          binding.getDestination(),
          binding.getRoutingKey(),
          e);
    }
  }
}
