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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
}
