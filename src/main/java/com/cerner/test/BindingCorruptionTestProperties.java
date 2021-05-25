package com.cerner.test;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("binding-corruption-test")
public class BindingCorruptionTestProperties {

  protected final List<Long> recoveryDelays = new ArrayList<>();
  protected int connections = 50;
  protected int channelsPerConnection = 10;
  protected int queuesPerChannel = 10;
  protected String topicExchange = "binding.corruption.test.topic";
  protected String queueNamePrefix = "binding-corruption-test-";
  protected String routingKeyPrefix = "BindingCorruptionTest.";
  protected boolean autoDelete = true;
  protected int maxTopologyRecoveryRetries = 10;
  protected String rabbitServiceName;

  public List<Long> getRecoveryDelays() {
    return recoveryDelays;
  }

  public int getConnections() {
    return connections;
  }

  public void setConnections(final int connections) {
    this.connections = connections;
  }

  public int getChannelsPerConnection() {
    return channelsPerConnection;
  }

  public void setChannelsPerConnection(final int channelsPerConnection) {
    this.channelsPerConnection = channelsPerConnection;
  }

  public int getQueuesPerChannel() {
    return queuesPerChannel;
  }

  public void setQueuesPerChannel(final int queuesPerChannel) {
    this.queuesPerChannel = queuesPerChannel;
  }

  public String getQueueNamePrefix() {
    return queueNamePrefix;
  }

  public void setQueueNamePrefix(final String queueNamePrefix) {
    this.queueNamePrefix = queueNamePrefix;
  }

  public String getRoutingKeyPrefix() {
    return routingKeyPrefix;
  }

  public void setRoutingKeyPrefix(final String routingKeyPrefix) {
    this.routingKeyPrefix = routingKeyPrefix;
  }

  public boolean isAutoDelete() {
    return autoDelete;
  }

  public void setAutoDelete(final boolean autoDelete) {
    this.autoDelete = autoDelete;
  }

  public String getRabbitServiceName() {
    return rabbitServiceName;
  }

  public void setRabbitServiceName(final String rabbitServiceName) {
    this.rabbitServiceName = rabbitServiceName;
  }

  public String getTopicExchange() {
    return topicExchange;
  }

  public void setTopicExchange(final String topicExchange) {
    this.topicExchange = topicExchange;
  }

  public int getMaxTopologyRecoveryRetries() {
    return maxTopologyRecoveryRetries;
  }

  public void setMaxTopologyRecoveryRetries(final int maxTopologyRecoveryRetries) {
    this.maxTopologyRecoveryRetries = maxTopologyRecoveryRetries;
  }
}
