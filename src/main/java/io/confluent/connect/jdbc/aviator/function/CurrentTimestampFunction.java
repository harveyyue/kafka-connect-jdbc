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

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.type.AviatorLong;
import com.googlecode.aviator.runtime.type.AviatorObject;
import io.confluent.connect.jdbc.aviator.AviatorDateUtils;

import java.util.Map;

public class CurrentTimestampFunction extends AbstractFunction {

  @Override
  public AviatorObject call(Map<String, Object> env) {
    return AviatorLong.valueOf(AviatorDateUtils.currentTimeMillis());
  }

  @Override
  public String getName() {
    return "current_timestamp";
  }
}
