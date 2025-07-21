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
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.TableShardDefinition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractDbWriter implements DbWriter {
  private static final Logger log = LoggerFactory.getLogger(AbstractDbWriter.class);

  protected final JdbcSinkConfig config;
  protected final DatabaseDialect dbDialect;
  protected final DbStructure dbStructure;
  protected final CachedConnectionProvider cachedConnectionProvider;

  final Map<TableId, TableId> tableIdMapping = new HashMap<>();
  final Map<String, TableId> topicToTableIdCache = new HashMap<>();

  public AbstractDbWriter(
      final JdbcSinkConfig config,
      DatabaseDialect dbDialect,
      DbStructure dbStructure
  ) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs
    );
    config.rawTableIdMapping.forEach((k, v) ->
        tableIdMapping.put(
            dbDialect.parseTableIdentifier(k), dbDialect.parseTableIdentifier(v)));
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("DbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  @Override
  public void closeQuietly() {
    cachedConnectionProvider.close();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit() {
    return Collections.emptyMap();
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

  protected TableId getTableId(SinkRecord record) {
    TableId tableId;
    TableShardDefinition tableShardDefinition =
        config.tableShardDefinitions.get(record.topic());
    if (tableShardDefinition != null) {
      Object rawValue = ((Struct) record.value()).get(tableShardDefinition.getShardColumn());
      if (rawValue == null) {
        throw new ConnectException("Not specified shard column value in topic " + record.topic());
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
}
