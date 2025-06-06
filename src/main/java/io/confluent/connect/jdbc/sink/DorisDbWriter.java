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
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.SQLException;
import java.util.Collection;

public class DorisDbWriter extends AbstractDbWriter {

  public DorisDbWriter(JdbcSinkConfig config, DatabaseDialect dbDialect) {
    super(config, dbDialect);
  }

  @Override
  public void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
  }

  @Override
  public void closeQuietly() {
  }
}
