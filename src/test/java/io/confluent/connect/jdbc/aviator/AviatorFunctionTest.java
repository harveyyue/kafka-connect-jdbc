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

package io.confluent.connect.jdbc.aviator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.googlecode.aviator.AviatorEvaluator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AviatorFunctionTest {
  private static final String INPUT_DATETIME = "2023-03-27T00:00:00+00:00";

  @Before
  public void init() {
    AviatorDateUtils.setCurrentDateTime(INPUT_DATETIME);
  }

  @Test
  public void testDateFunction() {
    String date = (String) AviatorHelper.execute("date()");
    assertEquals("2023-03-27", date);

    date = (String) AviatorHelper.execute("date(1)");
    assertEquals("2023-03-28", date);

    date = (String) AviatorHelper.execute("date(-1)");
    assertEquals("2023-03-26", date);

    date = (String) AviatorHelper.execute("date(0, 'yyyyMMddHH')");
    assertEquals("2023032700", date);

    date = (String) AviatorHelper.execute("date(0, 'yyyy-MM-dd HH:mm:ss')");
    assertEquals("2023-03-27 00:00:00", date);

    AviatorDateUtils.removeCurrentDateTime();
    date = (String) AviatorHelper.execute("date()");
    assertNotNull(date);
  }

  @Test
  public void testCurrentTimestampFunction() {
    long timestamp = (long) AviatorHelper.execute("current_timestamp()");
    assertEquals(1679875200000L, timestamp);

    AviatorDateUtils.removeCurrentDateTime();
    long currentTimestamp = (long) AviatorHelper.execute("current_timestamp()");
    assertEquals(13, String.valueOf(timestamp).length());
    assertNotEquals(currentTimestamp, timestamp);
  }

  @Test
  public void testJsonValueFunction() throws IOException {
    String json = "{\"id\":1,\"name\":\"json\",\"session\": {\"session_id\": \"abc123\"}}";
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonNode = objectMapper.readValue(json, JsonNode.class);
    Map<String, Object> env = new HashMap<>();
    env.put("obj", jsonNode);

    Object value = AviatorHelper.execute("json_value(obj, '$.session.session_id')", env);
    assertEquals("abc123", ((TextNode) value).asText());

    Object nestedPathValue = AviatorHelper.execute("json_value(obj, '$.session.session_id_')", env);
    assertTrue(((JsonNode) nestedPathValue).isNull());

    Object pathValue = AviatorHelper.execute("json_value(obj, '$.name2')", env);
    assertTrue(((JsonNode) pathValue).isNull());
  }

  @Test
  public void testExpressionFunction() {
    String expression = "if site_id == 'EU' { site_id } else { '' }";
    String value = (String) AviatorHelper.execute(expression, AviatorEvaluator.newEnv("site_id", "EU"));
    assertEquals("EU", value);
  }
}
