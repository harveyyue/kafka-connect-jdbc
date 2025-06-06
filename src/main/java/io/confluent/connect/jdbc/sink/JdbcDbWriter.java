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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.doris.DorisBufferedRecords;
import io.confluent.connect.jdbc.sink.doris.DorisRestService;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.TableShardDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;
  final Map<TableId, TableId> tableIdMapping = new HashMap<>();
  final Map<String, TableId> topicToTableIdCache = new HashMap<>();
  private DorisRestService dorisRestService;

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
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
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    try {
      final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
      for (SinkRecord record : records) {
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
        BufferedRecords buffer = bufferByTable.computeIfAbsent(tableId, key -> {
          if (dbDialect.name().equalsIgnoreCase("doris")) {
            if (dorisRestService == null) {
              dorisRestService = new DorisRestService(config);
            }
            return new DorisBufferedRecords(
                config, tableId, dbDialect, dbStructure, connection, dorisRestService);
          }
          return new JdbcBufferedRecords(config, tableId, dbDialect, dbStructure, connection);
        });
        buffer.add(record);
      }
      for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
        TableId tableId = entry.getKey();
        BufferedRecords buffer = entry.getValue();
        log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
        buffer.flush();
        buffer.close();
      }
      connection.commit();
    } catch (SQLException | TableAlterOrCreateException e) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableId destinationTable(String topic) {
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

  TableId destinationTable(TableShardDefinition tableShardDefinition, long millis) {
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
