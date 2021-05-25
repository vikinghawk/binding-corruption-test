package com.cerner.test;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("binding-corruption-detector")
public class BindingCorruptionDetectorProperties {

  private long waitMillis = 30000;
  private boolean repairCorruptBindings = true;
  private String httpApiUri;

  public long getWaitMillis() {
    return waitMillis;
  }

  public void setWaitMillis(final long waitMillis) {
    this.waitMillis = waitMillis;
  }

  public boolean isRepairCorruptBindings() {
    return repairCorruptBindings;
  }

  public void setRepairCorruptBindings(final boolean repairCorruptBindings) {
    this.repairCorruptBindings = repairCorruptBindings;
  }

  public String getHttpApiUri() {
    return httpApiUri;
  }

  public void setHttpApiUri(final String httpApiUri) {
    this.httpApiUri = httpApiUri;
  }
}
