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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class AviatorDateUtils {

  public static final ThreadLocal<ZonedDateTime> CURRENT_DATE_TIME = new ThreadLocal<>();

  /**
   * Set the date time string format from an external ecosystem
   *
   * @param dateTimeStr the string format like: 2023-03-27T00:00:00+00:00
   */
  public static void setCurrentDateTime(String dateTimeStr) {
    ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateTimeStr);
    CURRENT_DATE_TIME.set(zonedDateTime);
  }

  public static void setCurrentDateTime(ZonedDateTime zonedDateTime) {
    CURRENT_DATE_TIME.set(zonedDateTime);
  }

  public static void removeCurrentDateTime() {
    CURRENT_DATE_TIME.remove();
  }

  public static ZonedDateTime getZonedDateTime() {
    if (CURRENT_DATE_TIME.get() != null) {
      return CURRENT_DATE_TIME.get();
    }
    return ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC"));
  }

  public static String addDate(int number, String pattern) {
    return getZonedDateTime().plusDays(number).format(DateTimeFormatter.ofPattern(pattern));
  }
}
