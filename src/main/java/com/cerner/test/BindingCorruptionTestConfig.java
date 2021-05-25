package com.cerner.test;

import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
  RabbitProperties.class,
  BindingCorruptionTestProperties.class,
  BindingCorruptionDetectorProperties.class
})
public class BindingCorruptionTestConfig {

  @Bean
  @ConditionalOnProperty(
      value = "binding-corruption-test.enabled",
      havingValue = "true",
      matchIfMissing = false)
  public BindingCorruptionTest bindingCorruptionTest(
      final RabbitProperties rabbitProperties, final BindingCorruptionTestProperties testProps) {
    return new BindingCorruptionTest(rabbitProperties, testProps);
  }

  @Bean
  @ConditionalOnProperty(
      value = "binding-corruption-detector.enabled",
      havingValue = "true",
      matchIfMissing = false)
  public BindingCorruptionDetector bindingCorruptionDetector(
      final RabbitProperties rabbitProperties,
      final BindingCorruptionDetectorProperties detectorProps,
      final BindingCorruptionTestProperties testProps) {
    return new BindingCorruptionDetector(rabbitProperties, detectorProps, testProps);
  }
}
