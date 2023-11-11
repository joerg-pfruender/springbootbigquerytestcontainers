/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import com.example.config.GcpConfigurationProperties;
import com.example.initializer.BigQueryInitializer;
import com.google.cloud.bigquery.*;
import com.google.cloud.spring.bigquery.core.BigQueryTemplate;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * copied from
 * https://github.com/GoogleCloudPlatform/spring-cloud-gcp/blob/main/spring-cloud-gcp-samples/spring-cloud-gcp-bigquery-sample/src/test/java/com/example/BigQuerySampleApplicationIntegrationTests.java
 * with adaptions for usage with testcontainers https://java.testcontainers.org/modules/gcloud/
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = BigQuerySampleApplication.class,
        properties = "spring.cloud.gcp.bigquery.datasetName=test_dataset")
@ActiveProfiles("test")
@ContextConfiguration(initializers = BigQueryInitializer.class, classes = GcpConfigurationProperties.class)
class BigQuerySampleApplicationIntegrationTests {

  private static final String DATASET_NAME = GcpConfigurationProperties.DATASET_NAME;

  private static final String TABLE_NAME_PREFIX = "bigquery_sample_test_table";

  private static String TABLE_NAME;

  @Autowired
  BigQuery bigQuery;

  @Autowired
  private BigQueryTemplate bigQueryTemplate;

  @AfterEach
  void cleanupTestEnvironment() {
    // Clear the previous dataset before beginning the test.
    this.bigQuery.delete(TableId.of(DATASET_NAME, TABLE_NAME));
  }

  @BeforeEach
  void setTableName() {
    // adds a 5 char pseudo rand number suffix to make the table name unique before every run
    TABLE_NAME = TABLE_NAME_PREFIX + getRandSuffix();
  }
  // returns a 5 char pseudo rand number suffix

  private String getRandSuffix() {
    return UUID.randomUUID().toString().substring(0, 5);
  }

  @Test
  void testWriteAndReadJsonToBigQuery() throws InterruptedException, ExecutionException {

    Schema schema = WebController.getDefaultSchema();

    String jsonTxt =
            """
                    {"CompanyName":"TALES","Description":"mark","SerialNumber":97,"Leave":0,"EmpName":"Mark"}
                    {"CompanyName":"1Q84","Description":"ark","SerialNumber":978,"Leave":0,"EmpName":"HARUKI"}
                    {"CompanyName":"MY","Description":"M","SerialNumber":9780,"Leave":0,"EmpName":"Mark"}""";


    bigQuery.create(DatasetInfo.of(DATASET_NAME));
    bigQuery.create(TableInfo.newBuilder(TableId.of(DATASET_NAME, TABLE_NAME), StandardTableDefinition.of(schema)).build());

    CompletableFuture<Job> writeApiRes = this.bigQueryTemplate.writeDataToTable(
            TABLE_NAME, IOUtils.toInputStream(jsonTxt, "UTF-8"), FormatOptions.json(), schema);

    Job writeApiResponse = writeApiRes.get();
    assertThat(writeApiResponse.isDone()).isTrue();

    QueryJobConfiguration queryJobConfiguration =
            QueryJobConfiguration.newBuilder(
                            "SELECT * FROM " + DATASET_NAME + "." + TABLE_NAME + " order by SerialNumber desc")
                    .build();

    TableResult queryResult = this.bigQuery.query(queryJobConfiguration);
    assertThat(queryResult.getTotalRows()).isEqualTo(3);
    FieldValueList row = queryResult.getValues().iterator().next(); // match the first record
    assertThat(row.get("SerialNumber").getLongValue()).isEqualTo(9780L);
    assertThat(row.get("EmpName").getStringValue()).isEqualTo("Mark");
  }

}
