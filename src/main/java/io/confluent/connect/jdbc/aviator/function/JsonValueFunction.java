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

package io.confluent.connect.jdbc.aviator.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorRuntimeJavaType;
import io.confluent.connect.jdbc.util.JsonUtils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

public class JsonValueFunction extends AbstractFunction {

  public static final String FUNCTION_NAME = "json_value";
  private static final String PATTERN_PREFIX = "$.";
  private static final Pattern DOT_PATTERN = Pattern.compile("\\.");

  /**
   * function like: json_value(question, '$.distinctId')
   */
  @Override
  public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
    JsonNode root = (JsonNode) FunctionUtils.getJavaObject(arg1, env);
    if (root == null) {
      return AviatorRuntimeJavaType.valueOf(null);
    }

    JsonNode result = NullNode.getInstance();
    String pattern = FunctionUtils.getStringValue(arg2, env);
    if (pattern.startsWith(PATTERN_PREFIX)) {
      pattern = pattern.substring(PATTERN_PREFIX.length());
    }
    String[] elements = DOT_PATTERN.split(pattern);
    Iterator<String> iterator = Arrays.stream(elements).iterator();
    while (iterator.hasNext()) {
      String element = iterator.next();
      result = root.get(element);
      if (result == null || result.isNull()) {
        break;
      }
      root = result;
    }
    return AviatorRuntimeJavaType.valueOf(JsonUtils.as(result));
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }
}
