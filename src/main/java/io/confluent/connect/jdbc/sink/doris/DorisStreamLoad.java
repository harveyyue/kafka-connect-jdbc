/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink.doris;

import io.confluent.connect.jdbc.sink.doris.entity.StreamLoadContent;
import io.confluent.connect.jdbc.sink.doris.exception.DorisStreamLoadException;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DorisStreamLoad {
  private static final Logger log = LoggerFactory.getLogger(DorisStreamLoad.class);

  private static final String STREAM_LOAD_URL = "http://%s/api/%s/%s/_stream_load";
  private static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
  private static final List<String> DORIS_SUCCESS_STATUS =
      Arrays.asList("Success", "Publish Timeout");

  private final DorisRestService dorisRestService;
  private final TableId tableId;
  private final String beHostPort;

  public DorisStreamLoad(DorisRestService dorisRestService, TableId tableId) {
    this.dorisRestService = dorisRestService;
    this.tableId = tableId;
    this.beHostPort = dorisRestService.getBeHostPort();
  }

  public void load(String label, BatchBufferHttpEntity entity) {
    String url =
        String.format(STREAM_LOAD_URL, beHostPort, tableId.catalogName(), tableId.tableName());
    HttpPut httpPut = new HttpPut(url);
    httpPut.setHeader(dorisRestService.baseAuth());
    httpPut.addHeader("label", label);
    httpPut.addHeader(HttpHeaders.EXPECT, "100-continue");
    httpPut.addHeader("hidden_columns", DORIS_DELETE_SIGN);
    httpPut.addHeader("READ_JSON_BY_LINE", "true");
    httpPut.addHeader("format", "json");
    httpPut.setEntity(entity);

    try (CloseableHttpResponse response = dorisRestService.getHttpClient().execute(httpPut)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == 200 && response.getEntity() != null) {
        String loadResult = EntityUtils.toString(response.getEntity());
        log.info("Doris stream load response: {}", loadResult);
        StreamLoadContent content =
            dorisRestService.getObjectMapper().readValue(loadResult, StreamLoadContent.class);
        if (!DORIS_SUCCESS_STATUS.contains(content.getStatus())) {
          String reason = response.getStatusLine().getReasonPhrase();
          String message = String.format(
              "Doris stream load failed with %s, reason %s, stream load table: %s, status: %s",
              beHostPort,
              reason,
              tableId,
              content.getStatus());
          throw new DorisStreamLoadException(message);
        }
      }
    } catch (IOException e) {
      throw new DorisStreamLoadException(
          String.format("Doris stream load failed with %s, table: %s", beHostPort, tableId), e);
    }
  }
}
