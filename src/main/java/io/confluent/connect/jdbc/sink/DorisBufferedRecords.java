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
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DorisBufferedRecords extends AbstractBufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(DorisBufferedRecords.class);

  public DorisBufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    super(config, tableId, dbDialect, dbStructure, connection);
  }

  @Override
  public List<SinkRecord> add(SinkRecord record) throws SQLException, TableAlterOrCreateException {
    recordValidator.validate(record);
    final List<SinkRecord> flushed = new ArrayList<>();

    return flushed;
  }

  @Override
  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }

    log.debug("Flushing {} buffered records", records.size());
    // TODO: flush data
    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    return flushedRecords;
  }

  @Override
  public void close() throws SQLException {

  }
}
