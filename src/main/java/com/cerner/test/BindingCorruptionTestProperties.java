package com.cerner.test;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("binding-corruption-test")
public class BindingCorruptionTestProperties {

  protected final List<Long> recoveryDelays = new ArrayList<>();
  protected int connections = 100;
  protected int channelsPerConnection = 10;
  protected int queuesPerChannel = 10;
  protected String topicExchange = "binding.corruption.test.topic";
  protected String queueNamePrefix = "binding-corruption-test-";
  protected String routingKeyPrefix = "BindingCorruptionTest.";
  protected boolean autoDelete = true;
  protected int maxTopologyRecoveryRetries = 100;
  protected int maxConnectionResetRecoveryRetries = 0;
  protected String rabbitServiceName;
  protected long publishInterval = 5000;
  protected int publishThreads = 5;
}
