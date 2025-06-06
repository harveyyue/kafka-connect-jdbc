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

package io.confluent.connect.jdbc.sink.doris.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackendV2 {

  @JsonProperty(value = "backends")
  private List<BackendRowV2> backends;

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class BackendRowV2 {
    @JsonProperty("ip")
    public String ip;

    @JsonProperty("http_port")
    public int httpPort;

    @JsonProperty("is_alive")
    public boolean isAlive;

    public String toBackendString() {
      return ip + ":" + httpPort;
    }

    public static BackendRowV2 of(String ip, int httpPort, boolean alive) {
      BackendRowV2 rowV2 = new BackendRowV2();
      rowV2.setIp(ip);
      rowV2.setHttpPort(httpPort);
      rowV2.setAlive(alive);
      return rowV2;
    }
  }
}
