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
import io.confluent.connect.jdbc.sink.AbstractBufferedRecords;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.TableAlterOrCreateException;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class DorisBufferedRecords extends AbstractBufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(DorisBufferedRecords.class);

  private static final String LINE_DELIMITER = "\n";

  private final DorisJsonConverter dorisJsonConverter;
  private final DorisStreamLoad dorisStreamLoad;
  private final LinkedList<byte[]> buffer;
  private final byte[] lineDelimiter;

  private boolean loadBatchFirstRecord = true;
  private long bufferSizeBytes = 0;
  private int numOfRecords = 0;

  public DorisBufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection,
      DorisRestService dorisRestService
  ) {
    super(config, tableId, dbDialect, dbStructure, connection);
    this.dorisJsonConverter = DorisJsonConverter.getInstance();
    this.dorisStreamLoad = new DorisStreamLoad(dorisRestService, tableId);
    this.buffer = new LinkedList<>();
    this.lineDelimiter = LINE_DELIMITER.getBytes();
  }

  @Override
  public List<SinkRecord> add(SinkRecord record) throws SQLException, TableAlterOrCreateException {
    recordValidator.validate(record);

    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      // Initialize at first added, or this is a real key schema changed.
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    if (isDeleted(record) && config.deleteEnabled) {
      // Only support debezium delete event
      throw new ConnectException(
          "Use 'rewrite' mode in io.debezium.transforms.ExtractNewRecordState transform");
    } else if (!Objects.equals(valueSchema, record.valueSchema())) {
      // Initialize at first added, or this is a real value schema changed.
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }

    // Handle schema changed if needed
    if (schemaChanged) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flush();
      close();

      // re-initialize everything that depends on the record schema
      fieldsMetadata = extractFieldsMetadata(record);
      dbStructure.createOrAmendIfNecessary(
          config,
          connection,
          tableId,
          fieldsMetadata
      );
    }

    insert(record);
    if (numOfRecords >= config.batchSize) {
      flush();
    }

    // No need to return SinkRecords
    return new ArrayList<>();
  }

  private int insert(SinkRecord record) {
    byte[] json =
        dorisJsonConverter.serialize(record.topic(), record.valueSchema(), record.value());
    int recordSize = json.length;
    if (loadBatchFirstRecord) {
      loadBatchFirstRecord = false;
    } else {
      buffer.add(lineDelimiter);
      bufferSizeBytes += lineDelimiter.length;
      recordSize += lineDelimiter.length;
    }
    buffer.add(json);
    bufferSizeBytes += json.length;
    numOfRecords++;
    return recordSize;
  }

  private String generateBatchLabel() {
    return String.format(
        "%s_%s_%d",
        tableId.catalogName(),
        tableId.tableName(),
        System.currentTimeMillis());
  }

  @Override
  public List<SinkRecord> flush() throws SQLException {
    if (buffer.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }

    log.debug("Flushing {} buffered records", numOfRecords);
    dorisStreamLoad.load(generateBatchLabel(), new BatchBufferHttpEntity(buffer, bufferSizeBytes));
    buffer.clear();
    return new ArrayList<>();
  }

  @Override
  public void close() throws SQLException {
    loadBatchFirstRecord = true;
    bufferSizeBytes = 0;
    numOfRecords = 0;
  }
}
