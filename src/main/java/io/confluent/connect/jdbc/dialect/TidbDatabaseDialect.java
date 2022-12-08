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

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DatabaseDialect} for TIDB.
 */
public class TidbDatabaseDialect extends MySqlDatabaseDialect {
  private static final String TIDB_ROWID = "tidb_rowid";
  private static final String READONLY_ORIGIN_PRIMARY_KEY = "readonly_origin_primary_key";

  /**
   * The provider for {@link TidbDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(TidbDatabaseDialect.class.getSimpleName(), "tidb");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new TidbDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public TidbDatabaseDialect(AbstractConfig config) {
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
    // add tidb random rowid
    List<SinkRecordField> adjustFields = new ArrayList<>();
    adjustFields.add(rowIdRecordField());
    adjustFields.addAll(fields);
    writeColumnsSpec(builder, adjustFields);
    if (!pkFieldNames.isEmpty()) {
      // pk clause
      builder.append(",");
      builder.append(System.lineSeparator());
      builder.append("PRIMARY KEY(");
      builder.appendColumnName(TIDB_ROWID);
      builder.append(") /*T![clustered_index] CLUSTERED */");
      // uk clause
      builder.append(",");
      builder.append(System.lineSeparator());
      builder.append("UNIQUE KEY ");
      builder.append(READONLY_ORIGIN_PRIMARY_KEY);
      builder.append(" (");
      builder.appendList()
          .delimitedBy(",")
          .transformedBy(ExpressionBuilder.quote())
          .of(pkFieldNames);
      builder.append(")");
    }
    builder.append(")");
    return builder.toString();
  }

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
  public void writeColumnSpec(
      ExpressionBuilder builder,
      SinkRecordField f
  ) {
    super.writeColumnSpec(builder, f);
    if (f.name().equalsIgnoreCase(TIDB_ROWID)) {
      builder.append(" /*T![auto_rand] AUTO_RANDOM */");
    }
  }

  @Override
  public Map<ColumnId, ColumnDefinition> describeColumns(
      Connection connection,
      String catalogPattern,
      String schemaPattern,
      String tablePattern,
      String columnPattern
  ) throws SQLException {
    Set<String> originPrimaryKeys = new HashSet<>();
    Map<ColumnId, ColumnDefinition> columnDefns = super.describeColumns(
        connection,
        catalogPattern,
        schemaPattern,
        tablePattern,
        columnPattern);
    try (ResultSet rs = connection.getMetaData().getIndexInfo(
        catalogPattern,
        schemaPattern,
        tablePattern,
        true,
        true)) {
      while (rs.next()) {
        final String indexName = rs.getString(6);
        if (indexName.equalsIgnoreCase(READONLY_ORIGIN_PRIMARY_KEY)) {
          final String columnName = rs.getString(9);
          originPrimaryKeys.add(columnName);
        }
      }
    }
    // adjust primary key columns
    ColumnId tidbRowidColumnId = null;
    for (Map.Entry<ColumnId, ColumnDefinition> entry : columnDefns.entrySet()) {
      if (entry.getKey().name().equalsIgnoreCase(TIDB_ROWID)) {
        tidbRowidColumnId = entry.getKey();
      }
      if (originPrimaryKeys.contains(entry.getKey().name())) {
        entry.getValue().setPrimaryKey(true);
      }
    }
    columnDefns.remove(tidbRowidColumnId);
    return columnDefns;
  }

  private SinkRecordField rowIdRecordField() {
    return new SinkRecordField(Schema.INT64_SCHEMA, TIDB_ROWID, true);
  }
}
