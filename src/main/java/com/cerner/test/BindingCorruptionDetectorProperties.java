package com.cerner.test;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("binding-corruption-detector")
public class BindingCorruptionDetectorProperties {

  private long waitMillis = 30000;
  private boolean repairCorruptBindings = true;
  private String httpApiUri;
}
