package com.example.config;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.spring.autoconfigure.bigquery.GcpBigQueryAutoConfiguration;
import com.google.cloud.spring.autoconfigure.bigquery.GcpBigQueryProperties;
import com.google.cloud.spring.bigquery.core.BigQueryTemplate;
import com.google.cloud.spring.core.UserAgentHeaderProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@TestConfiguration
@EnableConfigurationProperties(GcpBigQueryProperties.class)
public class GcpConfigurationProperties {

  public static final String DATASET_NAME = "test_dataset";

  @Value("${spring.cloud.gcp.bigquery.projectId}")
  String projectId;

  @Value("${spring.cloud.gcp.bigquery.httpHostAndPort}")
  String httpHostAndPort;

  @Bean
  @Primary
  public BigQuery bigQuery() {
    String url = "http://" + httpHostAndPort;
    BigQueryOptions options = BigQueryOptions
            .newBuilder()
            .setProjectId(projectId)
            .setHost(url)
            .setLocation(url)
            .setCredentials(NoCredentials.getInstance())
            .build();
    return options.getService();
  }

  @Bean
  @Primary
  public BigQueryWriteClient bigQueryWriteClient() throws IOException {
    return BigQueryWriteClient.create(BigQueryWriteSettings.newBuilder()
            .setCredentialsProvider(new NoCredentialsProvider())
            .setEndpoint(httpHostAndPort)
            .setQuotaProjectId(projectId)
            .setHeaderProvider(new UserAgentHeaderProvider(GcpBigQueryAutoConfiguration.class))
            .build());
  }

  @Bean
  @Primary
  public CredentialsProvider credentialsProvider() {
    return () -> NoCredentials.getInstance();
  }
}
