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

package io.confluent.connect.jdbc;

import io.confluent.connect.jdbc.util.ConfigUtils;
import io.confluent.connect.jdbc.util.QuoteMethod;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;

public abstract class JdbcConfig extends AbstractConfig {

  public static final String CONNECTION_PREFIX = "connection.";

  public static final String CONNECTION_URL_CONFIG = CONNECTION_PREFIX + "url";
  public static final String CONNECTION_URL_DOC =
      "JDBC connection URL.\n"
          + "For example: ``jdbc:oracle:thin:@localhost:1521:orclpdb1``, "
          + "``jdbc:mysql://localhost/db_name``, "
          + "``jdbc:sqlserver://localhost;instance=SQLEXPRESS;"
          + "databaseName=db_name``";
  public static final String CONNECTION_URL_DISPLAY = "JDBC URL";
  public static final String CONNECTION_URL_DEFAULT = "";

  public static final String CONNECTION_USER_CONFIG = CONNECTION_PREFIX + "user";
  public static final String CONNECTION_USER_DOC = "JDBC connection user.";
  public static final String CONNECTION_USER_DISPLAY = "JDBC User";

  public static final String CONNECTION_PASSWORD_CONFIG = CONNECTION_PREFIX + "password";
  public static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
  public static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

  public static final String CONNECTION_ATTEMPTS_CONFIG = CONNECTION_PREFIX + "attempts";
  public static final String CONNECTION_ATTEMPTS_DOC
      = "Maximum number of attempts to retrieve a valid JDBC connection. "
      + "Must be a positive integer.";
  public static final String CONNECTION_ATTEMPTS_DISPLAY = "JDBC connection attempts";
  public static final int CONNECTION_ATTEMPTS_DEFAULT = 3;

  public static final String CONNECTION_BACKOFF_CONFIG = CONNECTION_PREFIX + "backoff.ms";
  public static final String CONNECTION_BACKOFF_DOC
      = "Backoff time in milliseconds between connection attempts.";
  public static final String CONNECTION_BACKOFF_DISPLAY
      = "JDBC connection backoff in milliseconds";
  public static final long CONNECTION_BACKOFF_DEFAULT = 10000L;

  public static final String DIALECT_NAME_CONFIG = "dialect.name";
  public static final String DIALECT_NAME_DISPLAY = "Database Dialect";
  public static final String DIALECT_NAME_DEFAULT = "";
  public static final String DIALECT_NAME_DOC =
      "The name of the database dialect that should be used for this connector. By default this "
          + "is empty, and the connector automatically determines the dialect based upon the "
          + "JDBC connection URL. Use this if you want to override that behavior and use a "
          + "specific dialect. All properly-packaged dialects in the JDBC connector plugin "
          + "can be used.";

  public static final String DB_TIMEZONE_CONFIG = "db.timezone";
  public static final String DB_TIMEZONE_DEFAULT = "UTC";
  public static final String DB_TIMEZONE_CONFIG_DOC =
      "Name of the JDBC timezone used in the connector when "
          + "querying/inserting with time-based criteria. Defaults to UTC.";
  public static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB time zone";

  public static final String QUOTE_SQL_IDENTIFIERS_CONFIG = "quote.sql.identifiers";
  public static final String QUOTE_SQL_IDENTIFIERS_DEFAULT = QuoteMethod.ALWAYS.name().toString();
  public static final String QUOTE_SQL_IDENTIFIERS_DOC =
      "When to quote table names, column names, and other identifiers in SQL statements. "
          + "For backward compatibility, the default is ``always``.";
  public static final String QUOTE_SQL_IDENTIFIERS_DISPLAY = "Quote Identifiers";

  public static final String TASK_ID = "task.id";

  protected final String connectorName;
  private final String taskId;

  public JdbcConfig(ConfigDef configDef, Map<?, ?> props) {
    super(configDef, props);
    this.connectorName = ConfigUtils.connectorName(props);
    this.taskId = (String) props.get(TASK_ID);
  }

  public String getConnectorName() {
    return this.connectorName;
  }

  public String getTaskId() {
    return this.taskId;
  }

  public TimeZone timeZone() {
    String dbTimeZone = getString(DB_TIMEZONE_CONFIG);
    return TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
  }

  public abstract String getContextName();
}
