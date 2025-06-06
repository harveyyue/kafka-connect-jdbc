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

package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.TableShardDefinition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractDbWriter {
  protected final JdbcSinkConfig config;
  protected final DatabaseDialect dbDialect;

  private final Map<TableId, TableId> tableIdMapping = new HashMap<>();
  private final Map<String, TableId> topicToTableIdCache = new HashMap<>();

  public AbstractDbWriter(JdbcSinkConfig config, DatabaseDialect dbDialect) {
    this.config = config;
    this.dbDialect = dbDialect;
    config.rawTableIdMapping.forEach((k, v) ->
        tableIdMapping.put(dbDialect.parseTableIdentifier(k), dbDialect.parseTableIdentifier(v)));
  }

  public abstract void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException;

  public abstract void closeQuietly();

  protected TableId getTableId(final SinkRecord record) {
    TableId tableId;
    TableShardDefinition tableShardDefinition =
        config.tableShardDefinitions.get(record.topic());
    if (tableShardDefinition != null) {
      Object rawValue = ((Struct) record.value()).get(tableShardDefinition.getShardColumn());
      if (rawValue == null) {
        throw new ConnectException("Not specified shard column value in topic "
            + record.topic());
      }
      // raw value maybe come from connect class org.apache.kafka.connect.data.Timestamp
      if (rawValue instanceof Date) {
        rawValue = ((Date) rawValue).getTime();
      }
      tableId = destinationTable(tableShardDefinition, Long.parseLong(rawValue.toString()));
    } else {
      // non-table shard mode
      tableId = topicToTableIdCache.computeIfAbsent(record.topic(), topic -> {
        TableId currentTableId = destinationTable(topic);
        if (tableIdMapping.get(currentTableId) != null) {
          return tableIdMapping.get(currentTableId);
        }
        return currentTableId;
      });
    }
    return tableId;
  }

  private TableId destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat
      ));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }

  private TableId destinationTable(TableShardDefinition tableShardDefinition, long millis) {
    String topic = tableShardDefinition.getShardTopicName(millis);
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat
      ));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }
}
