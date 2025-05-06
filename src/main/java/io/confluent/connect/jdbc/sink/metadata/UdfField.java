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

import org.apache.kafka.connect.data.Schema;

import io.confluent.connect.jdbc.aviator.AviatorHelper;

public class UdfField {
  private final String table;
  private final String column;
  private final String udf;
  private final String type;
  private final Schema schema;

  public UdfField(String table, String column, String udf, String type) {
    this.table = table;
    this.column = column;
    this.udf = udf;
    this.type = type;
    this.schema = toSchema();
  }

  public Object execute() {
    return AviatorHelper.execute(udf);
  }

  public String table() {
    return table;
  }

  public String column() {
    return column;
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
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  @Override
  public String toString() {
    return "UdfField{"
        + "table=" + table
        + ", column=" + column
        + ", udf=" + udf
        + ", type=" + type
        + ", schema=" + schema
        + '}';
  }
}
