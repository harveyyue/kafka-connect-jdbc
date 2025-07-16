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
import com.fasterxml.jackson.databind.node.NullNode;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.doris.DorisJsonConverter;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.sink.metadata.UdfField;
import io.confluent.connect.jdbc.util.BuilderType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.UDF_COLUMN_LIST_CONFIG;
import static io.confluent.connect.jdbc.sink.doris.DorisJsonConverter.DEBEZIUM_DELETED_FIELD;
import static io.confluent.connect.jdbc.sink.doris.DorisJsonConverter.DORIS_DELETE_SIGN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
  public void testUdfColumnList() {
    String currentTimestampFunction = "common_account_transfer_records:sync_time:current_timestamp():INT64";
    String ifElseFunction = "common_account_transfer_records:site_id:if site_id == 'EU' { site_id } else { '' }:string:site_id";
    String javaValueFunction = "common_account_transfer_records:intent_lang:json_value(response, '$.intent.language'):string:response";
    String coalesceFunction = "common_account_transfer_records:intent_lang2:coalesce(json_value(response, '$.intent.language'), ''):string:response";
    String blankCoalesceFunction = "common_account_transfer_records:intent_lang3:coalesce(json_value(response, '$.intent.language2'), ''):string:response";
    String dateFunction = "test_table:_sink_timestamp:date(0,'yyyy-MM-dd HH:mm:ss'):string";
    String udf = String.format("%s|%s|%s|%s|%s|%s", currentTimestampFunction, ifElseFunction, javaValueFunction, coalesceFunction, blankCoalesceFunction, dateFunction);
    JdbcSinkConfig jdbcSinkConfig = sinkConfigWithUrl("jdbc:mysql://something", UDF_COLUMN_LIST_CONFIG, udf);
    List<UdfField> udfColumnList = jdbcSinkConfig.udfColumnList;
    assertEquals(6, udfColumnList.size());
    assertEquals(
        "UdfField{table=common_account_transfer_records, column=site_id, udf=if site_id == 'EU' { site_id } else { '' }, type=string, inputColumns=[site_id], schema=Schema{STRING}}",
        udfColumnList.get(1).toString());
    assertEquals("date(0,'yyyy-MM-dd HH:mm:ss')", udfColumnList.get(5).udf());

    // test udf functions
    UdfField ifElseUdfField = udfColumnList.get(1);
    UdfField jsonUdfField = udfColumnList.get(2);
    UdfField coalesceUdfField = udfColumnList.get(3);

    // avro
    String response = "{\"response_id\":\"r123\",\"intent\":{\"language\":\"en\"}}";
    Schema schema = SchemaBuilder.struct()
        .name("com.example.Person")
        .field("site_id", Schema.STRING_SCHEMA)
        .field("id", Schema.INT32_SCHEMA)
        .field("response", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("site_id", "EU")
        .put("id", 21)
        .put("response", response);

    String value = (String) ifElseUdfField.execute(struct);
    assertEquals("EU", value);
    assertTrue(ifElseUdfField.inputColumnsExist());
    assertTrue(ifElseUdfField.inputColumnsValidate(schema));
    assertEquals("en", jsonUdfField.execute(struct));
    assertEquals("en", coalesceUdfField.execute(struct));

    // test case-sensitive
    Struct caseSensitiveStruct = new Struct(schema).put("site_id", "eu").put("id", 21);
    value = (String) ifElseUdfField.execute(caseSensitiveStruct);
    assertEquals("", value);

    // test doris json converter
    JsonNode jsonNodeWithoutResponse = converter.convertToJson(schema, caseSensitiveStruct, udfColumnList);
    assertEquals(NullNode.getInstance(), jsonNodeWithoutResponse.get("intent_lang"));

    JsonNode jsonNode = converter.convertToJson(schema, struct, udfColumnList);
    assertEquals("EU", jsonNode.get("site_id").asText());
    assertEquals("en", jsonNode.get("intent_lang").asText());
    assertEquals("en", jsonNode.get("intent_lang2").asText());
    assertEquals("", jsonNode.get("intent_lang3").asText());
    assertNotNull(jsonNode.get("sync_time"));
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
  public void shouldBuildMultipleAlterWithoutDefaultValueStatement() {
    List<String> alteredStatements = dialect.buildAlterTable(tableId, alterFieldsWithoutDefaultValue());
    List<String> expectedStatements = Arrays.asList(
        "ALTER TABLE `myTable` ADD COLUMN `foo` INT NOT NULL DEFAULT '0'",
        "ALTER TABLE `myTable` ADD COLUMN `bar` STRING NOT NULL DEFAULT ''");
    assertEquals(2, alteredStatements.size());
    assertEquals(alteredStatements, expectedStatements);
  }

  @Test
  public void shouldBuildMultipleAlterModifyStatement() {
    List<String> alteredStatements = dialect.buildAlterTable(tableId, alterFields(), dialect.expressionBuilder(BuilderType.ALTER_MODIFY));
    List<String> expectedStatements = Arrays.asList(
        "ALTER TABLE `myTable` MODIFY COLUMN `foo` INT DEFAULT '42'",
        "ALTER TABLE `myTable` MODIFY COLUMN `bar` STRING DEFAULT 'I'm bar'");
    assertEquals(2, alteredStatements.size());
    assertEquals(alteredStatements, expectedStatements);
  }

  @Test
  public void shouldBuildMultipleAlterModifyWithoutDefaultValueStatement() {
    List<String> alteredStatements = dialect.buildAlterTable(tableId, alterFieldsWithoutDefaultValue(), dialect.expressionBuilder(BuilderType.ALTER_MODIFY));
    List<String> expectedStatements = Arrays.asList(
        "ALTER TABLE `myTable` MODIFY COLUMN `foo` INT NOT NULL DEFAULT '0'",
        "ALTER TABLE `myTable` MODIFY COLUMN `bar` STRING NOT NULL DEFAULT ''");
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

    JsonNode jsonNode = converter.convertToJson(schemaA, valueA, Collections.emptyList());
    assertNull(jsonNode.get(DEBEZIUM_DELETED_FIELD));
    assertEquals("0", jsonNode.get(DORIS_DELETE_SIGN).asText());
    byte[] bytes = converter.serialize(null, schemaA, valueA, Collections.emptyList());
    assertEquals(43, bytes.length);
  }

  protected List<SinkRecordField> alterFieldsWithoutDefaultValue() {
    List<SinkRecordField> alterFields = new ArrayList<>();
    alterFields.add(new SinkRecordField(
        SchemaBuilder.int32().build(), "foo", false));
    alterFields.add(new SinkRecordField(
        SchemaBuilder.string().build(), "bar", false));
    return alterFields;
  }
}
