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

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import io.confluent.connect.jdbc.aviator.function.CurrentTimestampFunction;
import io.confluent.connect.jdbc.aviator.function.DateFunction;
import io.confluent.connect.jdbc.aviator.function.JsonValueFunction;

import java.util.Map;

public class AviatorHelper {

  private static final AviatorEvaluatorInstance aviatorEvaluatorInstance =
      AviatorEvaluator.newInstance();

  static {
    aviatorEvaluatorInstance.useLRUExpressionCache(1000);
    aviatorEvaluatorInstance.setCachedExpressionByDefault(true);
    // register aviator functions
    aviatorEvaluatorInstance.addFunction(new DateFunction());
    aviatorEvaluatorInstance.addFunction(new CurrentTimestampFunction());
    aviatorEvaluatorInstance.addFunction(new JsonValueFunction());
  }

  /**
   * Execute aviator function
   *
   * @param functionExpression function name, eg: month(1, 'yyyy-MM')
   * @return return the result of {@link Object} type
   */
  public static Object execute(String functionExpression) {
    return aviatorEvaluatorInstance.execute(functionExpression);
  }

  public static Object execute(String expression, Map<String, Object> env) {
    return aviatorEvaluatorInstance.execute(expression, env);
  }
}
