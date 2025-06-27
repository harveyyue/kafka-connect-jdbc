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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.connect.errors.ConnectException;

public class JsonUtils {

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static Object get(JsonNode value, String fieldName) {
    return as(value.get(fieldName));
  }

  public static Object as(JsonNode inputNode) {
    Object val;
    if (inputNode == null || inputNode.isNull()) {
      val = null;
    } else if (inputNode instanceof TextNode) {
      val = inputNode.asText();
    } else if (inputNode instanceof IntNode || inputNode instanceof ShortNode) {
      val = inputNode.asInt();
    } else if (inputNode instanceof LongNode) {
      val = inputNode.asLong();
    } else if (inputNode instanceof FloatNode || inputNode instanceof DoubleNode) {
      val = inputNode.asDouble();
    } else if (inputNode instanceof BooleanNode) {
      val = inputNode.asBoolean();
    } else {
      throw new ConnectException("Unsupported json node type: " + inputNode);
    }
    return val;
  }
}
