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
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;

import java.sql.Connection;
import java.util.Collection;
import java.util.stream.Collectors;

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
}
