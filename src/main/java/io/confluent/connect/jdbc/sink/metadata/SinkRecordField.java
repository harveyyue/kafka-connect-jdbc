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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class SinkRecordField {

  private static final String LENGTH_FIELD = "length";
  private static final String SCALE_FIELD = "scale";
  private static final String SCHEMA_PARAMETER_COLUMN_TYPE = "__debezium.source.column.type";
  private static final String SCHEMA_PARAMETER_COLUMN_SIZE = "__debezium.source.column.length";
  private static final String SCHEMA_PARAMETER_COLUMN_PRECISION = "__debezium.source.column.scale";
  private static final String SCHEMA_PARAMETER_COLUMN_NAME = "__debezium.source.column.name";

  private final Schema schema;
  private final String name;
  private final boolean isPrimaryKey;

  public SinkRecordField(Schema schema, String name, boolean isPrimaryKey) {
    this.schema = schema;
    this.name = name;
    this.isPrimaryKey = isPrimaryKey;
  }

  public Schema schema() {
    return schema;
  }

  public String schemaName() {
    return schema.name();
  }

  public Map<String, String> schemaParameters() {
    return schema.parameters();
  }

  public Schema.Type schemaType() {
    return schema.type();
  }

  public String name() {
    return name;
  }

  public boolean isOptional() {
    return !isPrimaryKey && schema.isOptional();
  }

  public Object defaultValue() {
    return schema.defaultValue();
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public Optional<String> getSourceColumnType() {
    return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_TYPE);
  }

  public Optional<Integer> getSourceColumnSize() {
    Optional<Integer> lengthOpt = getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_SIZE)
        .map(Integer::parseInt);
    if (lengthOpt.isPresent()) {
      return lengthOpt;
    }
    return getSchemaParameter(schema, LENGTH_FIELD).map(Integer::parseInt);
  }

  public Optional<Integer> getSourceColumnPrecision() {
    Optional<Integer> scaleOpt = getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_PRECISION)
        .map(Integer::parseInt);
    if (scaleOpt.isPresent()) {
      return scaleOpt;
    }
    return getSchemaParameter(schema, SCALE_FIELD).map(Integer::parseInt);
  }

  public Optional<String> getSourceColumnName() {
    return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_NAME);
  }

  private Optional<String> getSchemaParameter(Schema schema, String parameterName) {
    if (!Objects.isNull(schema.parameters())) {
      return Optional.ofNullable(schema.parameters().get(parameterName));
    }
    return Optional.empty();
  }

  @Override
  public String toString() {
    return "SinkRecordField{"
           + "schema=" + schema
           + ", name='" + name + '\''
           + ", isPrimaryKey=" + isPrimaryKey
           + '}';
  }
}
