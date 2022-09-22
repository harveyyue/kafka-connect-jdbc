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

package io.confluent.connect.jdbc.util;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TableShardDefinitionTest {

  @Test
  public void testParse() {
    String topicShardMapping = "test_topic:shard_ts:year, test_topic_2:shard_ts_2: month,";
    Map<String, TableShardDefinition> value = TableShardDefinition.parse(topicShardMapping);
    TableShardDefinition tableShardDefinition = value.get("test_topic");
    assertEquals(value.size(), 2);
    assertEquals(tableShardDefinition.getShardMode(), TableShardDefinition.TableShardMode.YEAR);
    assertEquals(tableShardDefinition.getShardColumn(), "shard_ts");
    assertEquals(tableShardDefinition.getShardTopicName(1663672374501L), "test_topic_2022");
    assertNotNull(value.get("test_topic_2"));
  }

  @Test
  public void testParseBlank() {
    Map<String, TableShardDefinition> value = TableShardDefinition.parse("");
    assertEquals(value.size(), 0);
  }

  @Test
  public void testFormatShard() {
    long timestamp = 1663672374501L;
    String value = TableShardDefinition.TableShardMode.DAY.shardFormat(timestamp);
    assertEquals(value, "20220920");
    // SECONDS SINCE JAN 01 1970.
    value = TableShardDefinition.TableShardMode.DAY.shardFormat(1663672374);
    assertEquals(value, "20220920");
  }
}
