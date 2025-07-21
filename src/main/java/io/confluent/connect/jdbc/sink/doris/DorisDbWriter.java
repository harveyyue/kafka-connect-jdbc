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

package io.confluent.connect.jdbc.sink.doris;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.AbstractDbWriter;
import io.confluent.connect.jdbc.sink.BufferedRecords;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.TableAlterOrCreateException;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DorisDbWriter extends AbstractDbWriter {
  private static final Logger log = LoggerFactory.getLogger(DorisDbWriter.class);

  private final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
  private final DorisRestService dorisRestService;

  public DorisDbWriter(
      final JdbcSinkConfig config,
      DatabaseDialect dbDialect,
      DbStructure dbStructure
  ) {
    super(config, dbDialect, dbStructure);
    this.dorisRestService = new DorisRestService(config);
  }

  @Override
  public void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    for (SinkRecord record : records) {
      TableId tableId = getTableId(record);
      BufferedRecords buffer = bufferByTable.computeIfAbsent(tableId, key -> {
        final Connection connection = cachedConnectionProvider.getConnection();
        return new DorisBufferedRecords(
            config, tableId, dbDialect, dbStructure, connection, dorisRestService);
      });
      buffer.add(record);
    }
  }

  @Override
  public void closeQuietly() {
    super.closeQuietly();

    bufferByTable.values().forEach(buffer -> {
      try {
        buffer.close();
      } catch (SQLException e) {
        log.error("Error while closing buffer", e);
      }
    });
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit() {
    Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
    bufferByTable.values().forEach(bufferedRecords ->
        ((DorisBufferedRecords) bufferedRecords)
            .offsetToCommit()
            .forEach((k, v) -> committed.put(k, new OffsetAndMetadata(v)))
    );
    return committed;
  }
}
