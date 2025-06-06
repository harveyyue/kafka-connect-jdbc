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
package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DatabaseDialect} for Doris.
 */
public class DorisDatabaseDialect extends GenericDatabaseDialect {

  private final Logger log = LoggerFactory.getLogger(DorisDatabaseDialect.class);

  /**
   * The provider for {@link DorisDatabaseDialect}.
   */
  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
    public Provider() {
      super(DorisDatabaseDialect.class.getSimpleName(), "doris");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new DorisDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public DorisDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "`", "`"));
  }

  @Override
  public TableId parseTableIdentifier(String fqn) {
    TableId tableId = super.parseTableIdentifier(fqn);
    // check topic naming mode
    return debeziumTableId(tableId);
  }

}

