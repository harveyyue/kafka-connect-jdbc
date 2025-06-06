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

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DorisDatabaseDialectTest extends BaseDialectTest<DorisDatabaseDialect> {

  @Override
  protected DorisDatabaseDialect createDialect() {
    return new DorisDatabaseDialect(sourceConfigWithUrl("jdbc:mysql://something"));
  }

  @Test
  public void testDescribeTable() throws Exception {
    JdbcSinkConfig jdbcSinkConfig = sinkConfigWithUrl(
        "jdbc:mysql://devtest-cht-cs-common-doris-cluster.master.devtest-storage.ww5sawfyut0k.bitsvc.io:9030",
        "connection.user", "bigdata_flink_rw", "connection.password", "wW7zKclv213OSg00");
    dialect = new DorisDatabaseDialect(jdbcSinkConfig);
    TableDefinition tableDefinition = dialect.describeTable(dialect.getConnection(), new TableId("rt_ods_asset", null, "deposit_records"));
    assertEquals(38, tableDefinition.columnCount());
  }
}
