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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.doris.entity.BackendV2;
import io.confluent.connect.jdbc.sink.doris.entity.DorisEntity;
import io.confluent.connect.jdbc.sink.doris.entity.Schema;
import io.confluent.connect.jdbc.sink.doris.exception.DorisConnectException;
import lombok.Getter;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Service for communicate with Doris FE.
 */
public class DorisRestService implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(DorisRestService.class);

  private static final Pattern DORIS_FE_HOST_PORT_PATTERN =
      Pattern.compile("jdbc:mysql://(.*):([0-9]+).*");
  private static final String BACKENDS_V2 = "/api/backends?is_alive=true";
  private static final String TABLE_SCHEMA_API = "/api/%s/%s/_schema";
  private static final String BE_HEALTH_URL = "http://%s/api/health";

  public static final String CONNECT_FAILED_MESSAGE = "Connect to doris {} failed.";
  public static final int DORIS_REQUEST_RETRIES_DEFAULT = 3;
  public static final int DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 60 * 1000;
  public static final int DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 60 * 1000;
  private static final Cache<String, String> BE_CACHE =
      Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).maximumSize(100).build();

  private final String feHostPort;
  private final String username;
  private final String password;
  @Getter
  private final CloseableHttpClient httpClient;
  @Getter
  private final ObjectMapper objectMapper;

  public DorisRestService(JdbcSinkConfig jdbcSinkConfig) {
    this.feHostPort = parseFeHostPort(jdbcSinkConfig);
    this.username = jdbcSinkConfig.connectionUser;
    this.password = jdbcSinkConfig.connectionPassword;
    this.httpClient = buildHttpClient();
    this.objectMapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  private String parseFeHostPort(JdbcSinkConfig jdbcSinkConfig) {
    String connectionUrl = jdbcSinkConfig.connectionUrl;
    Matcher matcher = DORIS_FE_HOST_PORT_PATTERN.matcher(connectionUrl);
    if (matcher.matches()) {
      String hostPort = String.format("http://%s:%d", matcher.group(1), jdbcSinkConfig.dorisFePort);
      return hostPort.endsWith("/") ? hostPort.substring(hostPort.length() - 1) : hostPort;
    }
    throw new DorisConnectException("Parse connection url failed: " + connectionUrl);
  }

  private CloseableHttpClient buildHttpClient() {
    return HttpClients.custom()
        .setRedirectStrategy(
            new DefaultRedirectStrategy() {
              @Override
              protected boolean isRedirectable(String method) {
                return true;
              }
            })
        .setDefaultRequestConfig(
            RequestConfig.custom()
                .setConnectTimeout(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                .setConnectionRequestTimeout(DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                .setSocketTimeout(DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                .build())
        .build();
  }

  /**
   * send request to Doris FE and get response json string.
   *
   * @param request {@link HttpRequestBase} real request
   * @return Doris FE response in json string
   * @throws DorisConnectException throw when cannot connect to Doris FE
   */
  private String send(HttpRequestBase request) {
    log.info(
        "Send request to Doris FE '{}' with user '{}'.",
        request.getURI(),
        username);
    IOException ex = null;
    int statusCode = -1;

    for (int attempt = 0; attempt < DORIS_REQUEST_RETRIES_DEFAULT; attempt++) {
      log.debug("Attempt {} to request {}.", attempt, request.getURI());
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 && response.getEntity() != null) {
          return EntityUtils.toString(response.getEntity());
        }

        long millis = attempt * 100;
        log.warn(
            "Failed to get response from Doris FE {}, http code is {}, reason {}, "
                + "will retry in {} ms.",
            request.getURI(),
            statusCode,
            response.getStatusLine().getReasonPhrase(),
            millis);
        // sleep a while
        try {
          Thread.sleep(millis);
        } catch (InterruptedException ie) {
          throw new DorisConnectException("Unexpect error.", ie);
        }
      } catch (IOException e) {
        ex = e;
      }
    }

    log.error(CONNECT_FAILED_MESSAGE, request.getURI(), ex);
    String message = String.format(
        "Connect to %s failed, status code is %d.",
        request.getURI().toString(),
        statusCode);
    throw new DorisConnectException(message, ex);
  }

  public List<BackendV2.BackendRowV2> getBackendsV2() {
    String raw = send(httpGet(feHostPort + BACKENDS_V2));
    try {
      DorisEntity<BackendV2> dorisEntity =
          objectMapper.readValue(raw, new TypeReference<DorisEntity<BackendV2>>() {
          });
      return dorisEntity.getData().getBackends();
    } catch (IOException e) {
      throw new DorisConnectException("Deserialization doris entity failed.", e);
    }
  }

  public String getBeHostPort() {
    return BE_CACHE.get(feHostPort, key -> {
      String beHostPort = null;
      List<BackendV2.BackendRowV2> backends = getBackendsV2();
      List<Integer> indexes =
          IntStream.range(0, backends.size()).boxed().collect(Collectors.toList());
      Collections.shuffle(indexes);
      for (Integer index : indexes) {
        beHostPort = backends.get(index).toBackendString();
        if (checkBeHealth(beHostPort)) {
          break;
        }
      }
      if (beHostPort == null) {
        throw new DorisConnectException("Connect doris be failed.");
      }
      return beHostPort;
    });
  }

  public Schema getTableSchema(String database, String table) {
    String tableSchemaUrl = String.format(feHostPort + TABLE_SCHEMA_API, database, table);
    String raw = send(httpGet(tableSchemaUrl));
    try {
      DorisEntity<Schema> dorisEntity =
          objectMapper.readValue(raw, new TypeReference<DorisEntity<Schema>>() {
          });
      return dorisEntity.getData();
    } catch (IOException e) {
      throw new DorisConnectException("Deserialization doris entity failed.", e);
    }
  }

  public boolean checkBeHealth(String beHostPort) {
    boolean healthy = false;
    try {
      String raw = send(httpGet(String.format(BE_HEALTH_URL, beHostPort)));
      log.info("Doris be {} health: {}", beHostPort, raw);
      if (raw != null && raw.toUpperCase().contains("OK")) {
        healthy = true;
      }
    } catch (DorisConnectException e) {
      log.error("Check doris be health error.", e);
    }
    return healthy;
  }

  private HttpGet httpGet(String uri) {
    HttpGet httpGet = new HttpGet(uri);
    httpGet.setHeader(baseAuth());
    return httpGet;
  }

  public BasicHeader baseAuth() {
    final String authInfo = username + ":" + password;
    byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
    return new BasicHeader("Authorization", "Basic " + new String(encoded));
  }
}
