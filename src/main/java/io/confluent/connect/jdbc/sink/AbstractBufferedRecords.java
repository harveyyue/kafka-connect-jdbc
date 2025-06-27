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
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.sink.metadata.UdfField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

public abstract class AbstractBufferedRecords implements BufferedRecords {

  protected final TableId tableId;
  protected final JdbcSinkConfig config;
  protected final DatabaseDialect dbDialect;
  protected final DbStructure dbStructure;
  protected final Connection connection;
  protected final RecordValidator recordValidator;

  protected Schema keySchema;
  protected Schema valueSchema;
  protected FieldsMetadata fieldsMetadata;

  public AbstractBufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    this.recordValidator = RecordValidator.create(config);
  }

  protected Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(tableId, name))
        .collect(Collectors.toList());
  }

  protected boolean isDeleted(SinkRecord record) {
    return isNull(record.value()) || isNull(record.valueSchema());
  }

  protected SchemaPair getSchemaPair(SinkRecord record) {
    return new SchemaPair(record.keySchema(), record.valueSchema());
  }

  protected FieldsMetadata extractFieldsMetadata(SinkRecord record) {
    SchemaPair schemaPair = getSchemaPair(record);
    // Add current table's udf columns if needed
    Map<String, UdfField> tableUdfFields =
        config.udfColumnList.stream()
            .filter(udfField -> udfField.table().equalsIgnoreCase(tableId.tableName()))
            .collect(Collectors.toMap(UdfField::column, udfField -> udfField));
    List<UdfField> invalidatedUdfFields =
        tableUdfFields.values().stream()
            .filter(udfField ->
                udfField.inputColumnsExist()
                    && !udfField.inputColumnsValidate(schemaPair.valueSchema))
            .collect(Collectors.toList());
    if (!invalidatedUdfFields.isEmpty()) {
      throw new ConnectException(
          String.format(
              "Udf fields does not exist in table %s, udf definition: %s",
              tableId,
              invalidatedUdfFields));
    }
    return FieldsMetadata.extract(
        tableId.tableName(),
        config.pkMode,
        config.pkFields,
        config.fieldsWhitelist,
        config.fieldsBlacklist,
        schemaPair,
        tableUdfFields
    );
  }
}
