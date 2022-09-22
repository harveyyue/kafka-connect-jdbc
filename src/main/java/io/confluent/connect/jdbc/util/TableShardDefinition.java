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

package io.confluent.connect.jdbc.util;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class TableShardDefinition {
  private String topicName;
  private String shardColumn;
  private TableShardMode shardMode;

  public TableShardDefinition(String topicName, String shardColumn, TableShardMode shardMode) {
    this.topicName = topicName;
    this.shardColumn = shardColumn;
    this.shardMode = shardMode;
  }

  public String getTopicName() {
    return topicName;
  }

  public String getShardColumn() {
    return shardColumn;
  }

  public TableShardMode getShardMode() {
    return shardMode;
  }

  public String getShardTopicName(long millis) {
    return String.format("%s_%s", topicName, shardMode.shardFormat(millis));
  }

  public static Map<String, TableShardDefinition> parse(String raw) {
    Map<String, TableShardDefinition> cache = new HashMap<>();
    if (StringUtils.isBlank(raw)) {
      return cache;
    }
    String[] modes = raw.split(",");
    List<String[]> topicModes = Arrays.stream(modes)
        .map(m -> m.trim().split(":"))
        .collect(Collectors.toList());
    if (topicModes.stream().filter(f -> f.length != 3
        || Arrays.stream(f).filter(StringUtils::isBlank).count() > 0).count() > 0) {
      throw new ConnectException("Couldn't start JdbcSinkConnector due to configuration error: "
          + JdbcSinkConfig.TABLE_SHARD_MAPPING_CONFIG + " value is invalid");
    }
    return topicModes.stream()
        .map(m -> new TableShardDefinition(
            m[0].trim(),
            m[1].trim(),
            TableShardMode.findTableShardMode(m[2].trim())))
        .collect(Collectors.toMap(TableShardDefinition::getTopicName, f -> f));
  }

  public enum TableShardMode {
    YEAR("yyyy"),
    MONTH("yyyyMM"),
    DAY("yyyyMMdd"),
    HOUR("yyyyMMddHH");

    private String format;

    TableShardMode(String format) {
      this.format = format;
    }

    public String shardFormat(long millis) {
      String value = DateFormatUtils.format(millis, format, TimeZone.getTimeZone("UTC"));
      if (value.startsWith("1970")) {
        // if seconds since JAN 01 1970
        value = DateFormatUtils.format(millis * 1000, format, TimeZone.getTimeZone("UTC"));
      }
      return value;
    }

    public static TableShardMode findTableShardMode(String value) {
      for (TableShardMode mode : values()) {
        if (mode.name().equalsIgnoreCase(value)) {
          return mode;
        }
      }
      throw new ConnectException("Couldn't find the table shard mode: " + value);
    }
  }
}
