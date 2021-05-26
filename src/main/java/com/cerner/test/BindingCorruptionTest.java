package com.cerner.test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

public class BindingCorruptionTest implements EnvironmentAware {

  private static final Logger log = LoggerFactory.getLogger(BindingCorruptionTest.class);

  private final RabbitProperties rabbitProps;
  private final BindingCorruptionTestProperties testProps;
  private final List<AutorecoveringConnection> connections = new ArrayList<>();
  private Environment environment;

  public BindingCorruptionTest(
      final RabbitProperties rabbitProps, final BindingCorruptionTestProperties testProps) {
    this.rabbitProps = rabbitProps;
    this.testProps = testProps;
  }

  @Override
  public void setEnvironment(final Environment environment) {
    this.environment = environment;
  }

  @PostConstruct
  public void start() throws Exception {
    Utils.initTasConfig(rabbitProps, environment, testProps, null);
    final String queuePrefix = testProps.getQueueNamePrefix() + System.currentTimeMillis() + "-";
    final String routingKeyPrefix =
        testProps.getRoutingKeyPrefix() + System.currentTimeMillis() + ".";
    int totalQueueCount = 0;
    boolean exchangeDeclared = false;
    for (int i = 1; i <= testProps.getConnections(); i++) {
      final String connectionName = "BindingCorruptionTest-" + i;
      final AutorecoveringConnection connection =
          Utils.createConnection(rabbitProps, testProps, connectionName);
      connections.add(connection);

      for (int j = 1; j <= testProps.getChannelsPerConnection(); j++) {
        final Channel channel = connection.createChannel();
        channel.addShutdownListener(
            cause -> {
              if (!cause.isHardError()
                  && !cause.isInitiatedByApplication()
                  && connection.isOpen()) {
                log.error(
                    "Error occurred on consumer channel={} for connection={}. Reason={}",
                    channel.getChannelNumber(),
                    connectionName,
                    cause.getReason(),
                    cause);
              }
            });
        channel.basicQos(20);
        if (!exchangeDeclared) {
          channel.exchangeDeclare(testProps.getTopicExchange(), BuiltinExchangeType.TOPIC, true);
          exchangeDeclared = true;
        }
        for (int k = 1; k <= testProps.getQueuesPerChannel(); k++) {
          final String queueName = queuePrefix + i + "-" + j + "-" + k;
          channel.queueDeclare(queueName, false, false, testProps.isAutoDelete(), null);
          channel.queueBind(
              queueName, testProps.getTopicExchange(), routingKeyPrefix + ++totalQueueCount);
          channel.basicConsume(
              queueName,
              new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(
                    final String consumerTag,
                    final Envelope envelope,
                    final BasicProperties properties,
                    final byte[] body)
                    throws IOException {
                  log.debug(
                      "Got message={} from connection={}, channel={}",
                      new String(body, StandardCharsets.UTF_8),
                      connectionName,
                      getChannel().getChannelNumber());
                  getChannel().basicAck(envelope.getDeliveryTag(), false);
                }
              });
        }
      }
    }
    log.info("Finished creating {} connections and {} queues", connections.size(), totalQueueCount);
  }

  @PreDestroy
  public void stop() {
    connections.forEach(
        c -> {
          try {
            c.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
    connections.clear();
    log.info("Closed {} connections", connections.size());
  }
}
