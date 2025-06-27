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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.connect.jdbc.sink.metadata.UdfField;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonSerializer;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DorisJsonConverter {
  public static final String DEBEZIUM_DELETED_FIELD = "__deleted";
  public static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
  public static final TextNode TEXT_NODE_ONE = new TextNode("1");
  public static final TextNode TEXT_NODE_ZERO = new TextNode("0");

  private final JsonConverter jsonConverter;
  private final Method convertToJsonMethod;
  private final JsonSerializer jsonSerializer;

  private static volatile DorisJsonConverter INSTANCE;

  public static DorisJsonConverter getInstance() {
    if (INSTANCE == null) {
      synchronized (DorisJsonConverter.class) {
        if (INSTANCE == null) {
          INSTANCE = new DorisJsonConverter();
        }
      }
    }
    return INSTANCE;
  }

  public DorisJsonConverter() {
    this.jsonConverter = new JsonConverter();
    Map<String, Object> configs = new HashMap<>();
    configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
    this.jsonConverter.configure(configs, false);
    Class<JsonConverter> clazz = JsonConverter.class;

    try {
      this.convertToJsonMethod =
          clazz.getDeclaredMethod("convertToJson", Schema.class, Object.class);
      this.convertToJsonMethod.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }

    try {
      Field serializerField = clazz.getDeclaredField("serializer");
      serializerField.setAccessible(true);
      this.jsonSerializer = (JsonSerializer) serializerField.get(jsonConverter);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object,
   * returning both the schema and the converted object.
   */
  public JsonNode convertToJson(Schema schema, Object value, Collection<UdfField> udfFields) {
    try {
      ObjectNode objectNode = (ObjectNode) convertToJsonMethod.invoke(jsonConverter, schema, value);
      if (objectNode.get(DEBEZIUM_DELETED_FIELD) != null) {
        objectNode.set(
            DORIS_DELETE_SIGN,
            objectNode.get(DEBEZIUM_DELETED_FIELD).asBoolean() ? TEXT_NODE_ONE : TEXT_NODE_ZERO);
        objectNode.remove(DEBEZIUM_DELETED_FIELD);
      } else {
        objectNode.set(DORIS_DELETE_SIGN, TEXT_NODE_ZERO);
      }
      // execute udf function
      for (UdfField udfField : udfFields) {
        Object obj =
            udfField.inputColumnsExist() ? udfField.execute(objectNode) : udfField.execute();
        objectNode.set(udfField.column(), toJsonNode(udfField.schema(), obj));
      }
      return objectNode;
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] serialize(
      String topic,
      Schema schema,
      Object value,
      Collection<UdfField> udfFields
  ) {
    return jsonSerializer.serialize(topic, convertToJson(schema, value, udfFields));
  }

  private JsonNode toJsonNode(Schema schema, Object value) {
    switch (schema.type()) {
      case STRING:
        return TextNode.valueOf((String) value);
      case INT8:
      case INT16:
      case INT32:
        return IntNode.valueOf((int) value);
      case INT64:
        return LongNode.valueOf((long) value);
      case FLOAT32:
        return FloatNode.valueOf((float) value);
      case FLOAT64:
        return DoubleNode.valueOf((double) value);
      case BOOLEAN:
        return BooleanNode.valueOf((boolean) value);
      default:
        throw new ConnectException("Unsupported type: " + schema.type());
    }
  }
}
