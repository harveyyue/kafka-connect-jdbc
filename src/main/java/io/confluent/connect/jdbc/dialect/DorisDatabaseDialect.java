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

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.BuilderType;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@link DatabaseDialect} for Doris.
 */
public class DorisDatabaseDialect extends MySqlDatabaseDialect {

  /**
   * The provider for {@link DorisDatabaseDialect}.
   */
  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
    public Provider() {
      super(DorisDatabaseDialect.class.getSimpleName(), "doris");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new DorisDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public DorisDatabaseDialect(AbstractConfig config) {
    super(config);
  }

  @Override
  public String buildCreateTableStatement(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();

    final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    builder.append("CREATE TABLE ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);
    builder.append(") ENGINE=OLAP");

    if (!pkFieldNames.isEmpty()) {
      builder.append(System.lineSeparator());
      builder.append("UNIQUE KEY(");
      builder.appendList()
          .delimitedBy(",")
          .transformedBy(ExpressionBuilder.quote())
          .of(pkFieldNames);
      builder.append(")");

      builder.append(System.lineSeparator());
      builder.append("DISTRIBUTED BY HASH(");
      builder.appendList()
          .delimitedBy(",")
          .transformedBy(ExpressionBuilder.quote())
          .of(pkFieldNames);
      builder.append(") BUCKETS AUTO");
    }
    return builder.toString();
  }

  /**
   * Build the doris ALTER TABLE statement expression for the given table and its columns.
   * Doris alter table statement:
   * ALTER TABLE [database.]table table_name
   * ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
   * [AFTER column_name|FIRST]
   * [TO rollup_index_name]
   * [PROPERTIES ("key"="value", ...)]
   *
   * @param table  the identifier of the table; may not be null
   * @param fields the information about the fields in the sink records; may not be null
   * @return the ALTER TABLE statement; may not be null
   */
  @Override
  public List<String> buildAlterTable(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    final List<String> queries = new ArrayList<>(fields.size());
    for (SinkRecordField field : fields) {
      queries.addAll(super.buildAlterTable(table, Collections.singleton(field)));
    }
    return queries;
  }

  @Override
  public List<String> buildAlterTable(
      TableId table,
      Collection<SinkRecordField> fields,
      ExpressionBuilder builder
  ) {
    final List<String> queries = new ArrayList<>(fields.size());
    for (SinkRecordField field : fields) {
      queries.addAll(
          super.buildAlterTable(table, Collections.singleton(field), builder.renew(builder)));
    }
    return queries;
  }

  @Override
  public void writeColumnSpec(
      ExpressionBuilder builder,
      SinkRecordField f
  ) {
    builder.appendColumnName(f.name());
    builder.append(" ");
    builder.append(getSqlType(f));
    if (f.defaultValue() != null && !f.schemaType().equals(Schema.Type.BYTES)) {
      builder.append(" DEFAULT ");
      formatColumnValue(
          builder,
          f.schemaName(),
          f.schemaParameters(),
          f.schemaType(),
          f.defaultValue()
      );
    } else if (isColumnOptional(f)) {
      builder.append(" NULL");
    } else {
      // doris not support adding not null constraint column without default value
      if (builder.buildType != null
          && (builder.buildType.equals(BuilderType.ALTER_ADD)
          || builder.buildType.equals(BuilderType.ALTER_MODIFY))) {
        builder.append(" NOT NULL DEFAULT ");
        formatColumnDefaultValue(
            builder,
            f.schemaName(),
            f.schemaParameters(),
            f.schemaType()
        );
      } else {
        builder.append(" NOT NULL");
      }
    }
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaType().equals(Schema.Type.BYTES)) {
      return "STRING";
    }
    String sqlType = super.getSqlType(field);
    if (sqlType.equalsIgnoreCase("TEXT")
        || sqlType.equalsIgnoreCase("TIME(3)")) {
      // Currently, doris is not supported time type
      sqlType = "STRING";
    }
    return sqlType;
  }

  @Override
  protected void formatColumnValue(
      ExpressionBuilder builder,
      String schemaName,
      Map<String, String> schemaParameters,
      Schema.Type type,
      Object value
  ) {
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          builder.appendStringQuoted(value);
          return;
        case Date.LOGICAL_NAME:
          builder.appendStringQuoted(DateTimeUtils.formatDate((java.util.Date) value, timeZone));
          return;
        case Time.LOGICAL_NAME:
          builder.appendStringQuoted(DateTimeUtils.formatTime((java.util.Date) value, timeZone));
          return;
        case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
          builder.appendStringQuoted(
              DateTimeUtils.formatTimestamp((java.util.Date) value, timeZone)
          );
          return;
        default:
          // fall through to regular types
          break;
      }
    }
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
      case STRING:
        builder.appendStringQuoted(value);
        break;
      case BOOLEAN:
        // 1 & 0 for boolean is more portable rather than TRUE/FALSE
        builder.appendStringQuoted((Boolean) value ? '1' : '0');
        break;
      default:
        break;
    }
  }

  private void formatColumnDefaultValue(
      ExpressionBuilder builder,
      String schemaName,
      Map<String, String> schemaParameters,
      Schema.Type type
  ) {
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
      case BOOLEAN:
        builder.appendStringQuoted(0);
        break;
      default:
        builder.appendStringQuoted("");
        break;
    }
  }
}

