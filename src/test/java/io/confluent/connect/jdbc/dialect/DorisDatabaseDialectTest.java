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

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.connect.jdbc.sink.doris.DorisJsonConverter;
import io.confluent.connect.jdbc.util.AlterType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.confluent.connect.jdbc.sink.doris.DorisJsonConverter.DEBEZIUM_DELETED_FIELD;
import static io.confluent.connect.jdbc.sink.doris.DorisJsonConverter.DORIS_DELETE_SIGN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DorisDatabaseDialectTest extends BaseDialectTest<DorisDatabaseDialect> {

  private DorisJsonConverter converter;

  @Before
  public void setup() throws Exception {
    super.setup();
    converter = DorisJsonConverter.getInstance();
  }

  @Override
  protected DorisDatabaseDialect createDialect() {
    return new DorisDatabaseDialect(sourceConfigWithUrl("jdbc:mysql://something"));
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected =
        "CREATE TABLE `myTable` (\n" +
            "`c1` INT NOT NULL,\n" +
            "`c2` BIGINT NOT NULL,\n" +
            "`c3` VARCHAR(100) NOT NULL,\n" +
            "`c4` STRING NULL,\n" +
            "`c5` DATE DEFAULT '2001-03-15',\n" +
            "`c6` STRING DEFAULT '00:00:00.000',\n" +
            "`c7` DATETIME(3) DEFAULT '2001-03-15 00:00:00.000',\n" +
            "`c8` STRING NULL,\n" +
            "`c9` TINYINT DEFAULT '1') ENGINE=OLAP\n" +
            "UNIQUE KEY(`c1`)\n" +
            "DISTRIBUTED BY HASH(`c1`) BUCKETS AUTO";
    String sql = dialect.buildCreateTableStatement(tableId, buildSinkRecordFieldsIncludingSchemaParameters());
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildMultipleAlterStatement() {
    List<String> alteredStatements = dialect.buildAlterTable(tableId, alterFields());
    List<String> expectedStatements = Arrays.asList(
        "ALTER TABLE `myTable` ADD COLUMN `foo` INT DEFAULT '42'",
        "ALTER TABLE `myTable` ADD COLUMN `bar` STRING DEFAULT 'I'm bar'");
    assertEquals(2, alteredStatements.size());
    assertEquals(alteredStatements, expectedStatements);
  }

  @Test
  public void shouldBuildMultipleAlterModifyStatement() {
    List<String> alteredStatements = dialect.buildAlterTable(tableId, alterFields(), dialect.expressionBuilder().setAlterType(AlterType.MODIFY));
    List<String> expectedStatements = Arrays.asList(
        "ALTER TABLE `myTable` MODIFY COLUMN `foo` INT DEFAULT '42'",
        "ALTER TABLE `myTable` MODIFY COLUMN `bar` STRING DEFAULT 'I'm bar'");
    assertEquals(2, alteredStatements.size());
    assertEquals(alteredStatements, expectedStatements);
  }

  @Test
  public void testJsonConverter() {
    final Schema schemaA = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field(DEBEZIUM_DELETED_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();
    final Struct valueA = new Struct(schemaA)
        .put("name", "cuba")
        .put(DEBEZIUM_DELETED_FIELD, false);

    JsonNode jsonNode = converter.convertToJson(schemaA, valueA);
    assertNull(jsonNode.get(DEBEZIUM_DELETED_FIELD));
    assertEquals("0",  jsonNode.get(DORIS_DELETE_SIGN).asText());
    byte[] bytes = converter.serialize(null, schemaA, valueA);
    assertEquals(43, bytes.length);
  }
}
