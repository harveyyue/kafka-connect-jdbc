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

package io.confluent.connect.jdbc.sink.doris.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Schema {
  private int status;
  private String keysType;
  private List<Field> properties;

  public void put(
      String name,
      String type,
      String comment,
      int scale,
      int precision,
      String aggregationType) {
    properties.add(new Field(name, type, comment, scale, precision, aggregationType));
  }

  public void put(Field f) {
    properties.add(f);
  }

  public Field get(int index) {
    if (index >= properties.size()) {
      throw new IndexOutOfBoundsException(
          "Index: " + index + ", Fields size：" + properties.size());
    }
    return properties.get(index);
  }

  public int size() {
    return properties.size();
  }
}
