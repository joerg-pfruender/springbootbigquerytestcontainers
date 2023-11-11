package com.example.initializer;

import org.springframework.boot.test.util.TestPropertyValues;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

public class BigQueryInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  private BigQueryEmulatorContainer container = new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.4.3");

  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    container.start();
    TestPropertyValues testPropertyValues = TestPropertyValues.of(
            "BIGQUERY_PROJECT_ID="+container.getProjectId(),
            "BIGQUERY_HTTP_HOST_AND_PORT="+container.getEmulatorHttpHostAndPort()
    );
    testPropertyValues.applyTo(applicationContext);
  }
}
