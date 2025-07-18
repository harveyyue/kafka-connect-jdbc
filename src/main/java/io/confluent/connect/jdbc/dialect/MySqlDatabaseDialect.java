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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DatabaseDialect} for MySQL.
 */
public class MySqlDatabaseDialect extends GenericDatabaseDialect {

  private final Logger log = LoggerFactory.getLogger(MySqlDatabaseDialect.class);

  /**
   * The provider for {@link MySqlDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(MySqlDatabaseDialect.class.getSimpleName(), "mariadb", "mysql");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new MySqlDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public MySqlDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "`", "`"));
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
   * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
   * scroll more efficiently through the result set and prevent out of memory errors.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  @Override
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    super.initializePreparedStatement(stmt);

    log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Override
  public TableId parseTableIdentifier(String fqn) {
    TableId tableId = super.parseTableIdentifier(fqn);
    // check topic naming mode
    return debeziumTableId(tableId);
  }

  @Override
  public void writeColumnSpec(
      ExpressionBuilder builder,
      SinkRecordField f
  ) {
    builder.appendColumnName(f.name());
    builder.append(" ");
    String sqlType = getSqlType(f);
    builder.append(sqlType);
    // Mapping STRING to TEXT and always set null constraint for text in mysql
    if (f.defaultValue() != null && !f.schemaType().equals(Schema.Type.STRING)) {
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
      builder.append(" NOT NULL");
    }
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    Optional<Integer> lengthOpt = field.getSourceColumnSize();
    Optional<Integer> scaleOpt = field.getSourceColumnPrecision();
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          // Maximum precision supported by MySQL is 65
          Integer length = lengthOpt.orElse(65);
          return String.format("DECIMAL(%d, %d)", length, scaleOpt.get());
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME(3)";
        case Timestamp.LOGICAL_NAME:
          return "DATETIME(3)";
        default:
          // pass through to primitive types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        if (lengthOpt.isPresent() && scaleOpt.isPresent()) {
          return String.format("DOUBLE(%d,%d)", lengthOpt.get(), scaleOpt.get());
        }
        return "DOUBLE";
      case BOOLEAN:
        return "TINYINT";
      case STRING:
        Optional<String> dataTypeOpt = field.getSourceColumnType();
        if (dataTypeOpt.isPresent()) {
          String originType = dataTypeOpt.get().toUpperCase();
          // DECIMAL, NUMERIC and FIXED type contain keywords: UNSIGNED ZEROFILL
          if (originType.contains("DECIMAL")
              || originType.contains("NUMERIC")
              || originType.contains("FIXED")
              || originType.contains("TIMESTAMP")
              || originType.equals("ENUM")
              || originType.equals("SET")) {
            return "VARCHAR(255)";
          } else if (lengthOpt.isPresent()) {
            return String.format("%s(%d)", originType, lengthOpt.get());
          }
        }
        return "TEXT";
      case BYTES:
        return "VARBINARY(1024)";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildInsertIgnoreStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    String insert = super.buildInsertStatement(table, keyColumns, nonKeyColumns, definition);
    return insert.replaceFirst("INSERT INTO", "INSERT IGNORE INTO");
  }

  @Override
  public String buildUpsertIgnoreQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    String upsert = buildUpsertQueryStatement(table, keyColumns, nonKeyColumns);
    return upsert.replaceFirst("insert into", "insert ignore into");
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    //MySql doesn't support SQL 2003:merge so here how the upsert is handled
    final Transform<ColumnId> transform = (builder, col) -> {
      builder.appendColumnName(col.name());
      builder.append("=values(");
      builder.appendColumnName(col.name());
      builder.append(")");
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("insert into ");
    builder.append(table);
    builder.append("(");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns, nonKeyColumns);
    builder.append(") values(");
    builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
    builder.append(") on duplicate key update ");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(transform)
           .of(nonKeyColumns.isEmpty() ? keyColumns : nonKeyColumns);
    return builder.toString();
  }

  @Override
  protected String sanitizedUrl(String url) {
    // MySQL can also have "username:password@" at the beginning of the host list and
    // in parenthetical properties
    return super.sanitizedUrl(url)
                .replaceAll("(?i)([(,]password=)[^,)]*", "$1****")
                .replaceAll("(://[^:]*:)([^@]*)@", "$1****@");
  }
}
