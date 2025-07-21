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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DorisBufferedRecords extends AbstractBufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(DorisBufferedRecords.class);

  private static final String LINE_DELIMITER = "\n";

  private final Object obj = new Object();
  private final Map<TopicPartition, Long> currentOffsets = new HashMap<>();
  private final Map<TopicPartition, Long> offsetToCommit = new HashMap<>();
  private final BlockingQueue<byte[]> buffer = new ArrayBlockingQueue<>(config.batchSize);
  private final DorisJsonConverter dorisJsonConverter;
  private final DorisStreamLoad dorisStreamLoad;
  private final byte[] lineDelimiter;
  private final ScheduledExecutorService scheduler;

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
    this.lineDelimiter = LINE_DELIMITER.getBytes();
    this.scheduler = Executors.newSingleThreadScheduledExecutor(
        ThreadUtils.createThreadFactory(threadName(), false));

    scheduler.scheduleWithFixedDelay(
        this::doFlush,
        config.dorisBufferFlushIntervalMs,
        config.dorisBufferFlushIntervalMs,
        TimeUnit.MILLISECONDS);
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
    return Collections.emptyList();
  }

  private int insert(SinkRecord record) {
    byte[] json =
        dorisJsonConverter.serialize(
            record.topic(),
            record.valueSchema(),
            record.value(),
            fieldsMetadata.udfFields.values());

    synchronized (obj) {
      try {
        byte[] result;
        if (loadBatchFirstRecord) {
          result = json;
          loadBatchFirstRecord = false;
        } else {
          result = concat(lineDelimiter, json);
        }
        int recordSize = result.length;
        buffer.put(result);
        bufferSizeBytes += recordSize;
        numOfRecords++;
        currentOffsets.put(
            new TopicPartition(record.topic(), record.kafkaPartition()), record.kafkaOffset());
        return recordSize;
      } catch (InterruptedException e) {
        log.error("Unexpect error.", e);
        return 0;
      }
    }
  }

  private byte[] concat(byte[] first, byte[] second) {
    byte[] result = new byte[first.length + second.length];
    System.arraycopy(first, 0, result, 0, first.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  private String threadName() {
    String prefix = String.format(
        "%s-%s-%s-%s-doris-batch-load",
        config.getConnectorName(),
        config.getTaskId(),
        tableId.catalogName(),
        tableId.tableName());
    return prefix + "-%d";
  }

  private String generateBatchLabel() {
    return String.format(
        "%s_%s_%s_%s_%d",
        config.getConnectorName(),
        config.getTaskId(),
        tableId.catalogName(),
        tableId.tableName(),
        System.currentTimeMillis());
  }

  @Override
  public List<SinkRecord> flush() throws SQLException {
    doFlush();
    return Collections.emptyList();
  }

  private void doFlush() {
    if (buffer.isEmpty()) {
      log.debug("Records is empty");
      return;
    }

    log.debug("Flushing {} buffered records for table ID: {}", numOfRecords, tableId);
    synchronized (obj) {
      List<byte[]> batch = new ArrayList<>();
      buffer.drainTo(batch, buffer.size());
      dorisStreamLoad.load(generateBatchLabel(), new BatchBufferHttpEntity(batch, bufferSizeBytes));
      // cleanup
      loadBatchFirstRecord = true;
      bufferSizeBytes = 0;
      numOfRecords = 0;
      // committed offsets
      currentOffsets.forEach((k, v) -> offsetToCommit.put(k, v + 1));
    }
  }

  @Override
  public void close() throws SQLException {
    if (scheduler != null) {
      scheduler.shutdown();
    }
  }

  public Map<TopicPartition, Long> offsetToCommit() {
    return offsetToCommit;
  }
}
