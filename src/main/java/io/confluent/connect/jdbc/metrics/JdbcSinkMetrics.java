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

package io.confluent.connect.jdbc.metrics;

import io.confluent.connect.jdbc.JdbcConfig;

import java.util.concurrent.atomic.AtomicLong;

public class JdbcSinkMetrics extends Metrics implements JdbcMXBean {

  private final AtomicLong milliSecondsBehindSource = new AtomicLong();

  public JdbcSinkMetrics(JdbcConfig jdbcConfig) {
    super(jdbcConfig, "streaming");
  }

  @Override
  public long getMilliSecondsBehindSource() {
    return milliSecondsBehindSource.get();
  }

  public void setMilliSecondsBehindSource(long value) {
    milliSecondsBehindSource.set(value);
  }
}
