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

package io.confluent.connect.jdbc.sink.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import io.confluent.connect.jdbc.aviator.AviatorHelper;
import io.confluent.connect.jdbc.util.JsonUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.confluent.connect.jdbc.aviator.function.JsonValueFunction.JSON_VALUE_PATTERN;

public class UdfField {
  private static final ObjectMapper OBJECT_MAPPER = JsonUtils.OBJECT_MAPPER;

  private final String table;
  private final String column;
  private final String udf;
  private final String type;
  private final String[] inputColumns;
  private final Schema schema;

  public UdfField(String table, String column, String udf, String type) {
    this(table, column, udf, type, null);
  }

  public UdfField(String table, String column, String udf, String type, String[] inputColumns) {
    this.table = table;
    this.column = column;
    this.udf = udf;
    this.type = type;
    this.inputColumns = inputColumns;
    this.schema = toSchema();
  }

  public Object execute() {
    return AviatorHelper.execute(udf);
  }

  public Object execute(Struct value) {
    return execute(value, Struct::get);
  }

  public Object execute(JsonNode value) {
    return execute(value, JsonUtils::get);
  }

  public <T> Object execute(T value, BiFunction<T, String, Object> function) {
    Map<String, Object> env = new HashMap<>();
    // reuse value for json_value function if needed
    Map<String, JsonNode> cache = new HashMap<>();
    for (String inputColumn : inputColumns) {
      Object val = function.apply(value, inputColumn);
      if (JSON_VALUE_PATTERN.matcher(udf).matches()) {
        if (val == null) {
          val = NullNode.getInstance();
        } else {
          String finalVal = val.toString();
          val = cache.computeIfAbsent(inputColumn, k -> {
            try {
              return OBJECT_MAPPER.readValue(finalVal, JsonNode.class);
            } catch (IOException e) {
              throw new ConnectException(e);
            }
          });
        }
      }
      env.put(inputColumn, val);
    }
    return AviatorHelper.execute(udf, env);
  }

  public String table() {
    return table;
  }

  public String column() {
    return column;
  }

  public boolean inputColumnsExist() {
    return inputColumns != null && inputColumns.length > 0;
  }

  public boolean inputColumnsValidate(Schema schema) {
    List<String> fields =
        schema.fields().stream().map(Field::name).collect(Collectors.toList());
    for (String column : inputColumns) {
      if (!fields.contains(column)) {
        return false;
      }
    }
    return true;
  }

  public String udf() {
    return udf;
  }

  public Schema schema() {
    return schema;
  }

  private Schema toSchema() {
    switch (type.toUpperCase()) {
      case "INT8":
        return Schema.OPTIONAL_INT8_SCHEMA;
      case "INT16":
        return Schema.OPTIONAL_INT16_SCHEMA;
      case "INT":
        return Schema.OPTIONAL_INT32_SCHEMA;
      case "INT64":
        return Schema.OPTIONAL_INT64_SCHEMA;
      case "FLOAT32":
        return Schema.OPTIONAL_FLOAT32_SCHEMA;
      case "FLOAT64":
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      case "BOOLEAN":
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
      case "STRING":
        return Schema.OPTIONAL_STRING_SCHEMA;
      case "BYTES":
        return Schema.OPTIONAL_BYTES_SCHEMA;
      default:
        throw new ConnectException("Unsupported type: " + type);
    }
  }

  @Override
  public String toString() {
    return "UdfField{"
        + "table=" + table
        + ", column=" + column
        + ", udf=" + udf
        + ", type=" + type
        + ", inputColumns=" + Arrays.toString(inputColumns)
        + ", schema=" + schema
        + '}';
  }
}
