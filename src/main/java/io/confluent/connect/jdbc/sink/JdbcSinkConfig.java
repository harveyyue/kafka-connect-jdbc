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

package io.confluent.connect.jdbc.sink;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.JdbcConfig;
import io.confluent.connect.jdbc.sink.metadata.UdfField;
import io.confluent.connect.jdbc.util.DatabaseDialectRecommender;
import io.confluent.connect.jdbc.util.DeleteEnabledRecommender;
import io.confluent.connect.jdbc.util.EnumRecommender;
import io.confluent.connect.jdbc.util.PrimaryKeyModeRecommender;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.StringUtils;
import io.confluent.connect.jdbc.util.TableShardDefinition;
import io.confluent.connect.jdbc.util.TableType;
import io.confluent.connect.jdbc.util.TimeZoneValidator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSinkConfig extends JdbcConfig {

  public enum InsertMode {
    INSERT,
    INSERT_IGNORE,
    UPSERT,
    UPSERT_IGNORE,
    UPDATE;

  }

  public enum PrimaryKeyMode {
    NONE,
    KAFKA,
    RECORD_KEY,
    RECORD_VALUE;
  }

  public enum TopicNamingMode {
    NORMAL,
    DEBEZIUM
  }

  public static final List<String> DEFAULT_KAFKA_PK_NAMES = Collections.unmodifiableList(
      Arrays.asList(
          "__connect_topic",
          "__connect_partition",
          "__connect_offset"
      )
  );

  public static final String TABLE_NAME_FORMAT = "table.name.format";
  private static final String TABLE_NAME_FORMAT_DEFAULT = "${topic}";
  private static final String TABLE_NAME_FORMAT_DOC =
      "A format string for the destination table name, which may contain '${topic}' as a "
      + "placeholder for the originating topic name.\n"
      + "For example, ``kafka_${topic}`` for the topic 'orders' will map to the table name "
      + "'kafka_orders'.";
  private static final String TABLE_NAME_FORMAT_DISPLAY = "Table Name Format";

  public static final String MAX_RETRIES = "max.retries";
  private static final int MAX_RETRIES_DEFAULT = 10;
  private static final String MAX_RETRIES_DOC =
      "The maximum number of times to retry on errors before failing the task.";
  private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

  public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
  private static final String RETRY_BACKOFF_MS_DOC =
      "The time in milliseconds to wait following an error before a retry attempt is made.";
  private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

  public static final String BATCH_SIZE = "batch.size";
  private static final int BATCH_SIZE_DEFAULT = 3000;
  private static final String BATCH_SIZE_DOC =
      "Specifies how many records to attempt to batch together for insertion into the destination"
      + " table, when possible.";
  private static final String BATCH_SIZE_DISPLAY = "Batch Size";

  public static final String DELETE_ENABLED = "delete.enabled";
  private static final String DELETE_ENABLED_DEFAULT = "false";
  private static final String DELETE_ENABLED_DOC =
      "Whether to treat ``null`` record values as deletes. Requires ``pk.mode`` "
      + "to be ``record_key``.";
  private static final String DELETE_ENABLED_DISPLAY = "Enable deletes";

  public static final String AUTO_CREATE = "auto.create";
  private static final String AUTO_CREATE_DEFAULT = "false";
  private static final String AUTO_CREATE_DOC =
      "Whether to automatically create the destination table based on record schema if it is "
      + "found to be missing by issuing ``CREATE``.";
  private static final String AUTO_CREATE_DISPLAY = "Auto-Create";

  public static final String AUTO_EVOLVE = "auto.evolve";
  private static final String AUTO_EVOLVE_DEFAULT = "false";
  private static final String AUTO_EVOLVE_DOC =
      "Whether to automatically add columns in the table schema when found to be missing relative "
      + "to the record schema by issuing ``ALTER``.";
  private static final String AUTO_EVOLVE_DISPLAY = "Auto-Evolve";

  public static final String INSERT_MODE = "insert.mode";
  private static final String INSERT_MODE_DEFAULT = "insert";
  private static final String INSERT_MODE_DOC =
      "The insertion mode to use. Supported modes are:\n"
      + "``insert``\n"
      + "    Use standard SQL ``INSERT`` statements.\n"
      + "``insert_ignore``\n"
      + "    Use standard SQL ``INSERT IGNORE`` statements.\n"
      + "``upsert``\n"
      + "    Use the appropriate upsert semantics for the target database if it is supported by "
      + "the connector, e.g. ``INSERT OR IGNORE``.\n"
      + "``upsert_ignore``\n"
      + "    Use the appropriate upsert ignore semantics for MySQL/TiDB. \n"
      + "``update``\n"
      + "    Use the appropriate update semantics for the target database if it is supported by "
      + "the connector, e.g. ``UPDATE``.";
  private static final String INSERT_MODE_DISPLAY = "Insert Mode";

  public static final String PK_FIELDS = "pk.fields";
  private static final String PK_FIELDS_DEFAULT = "";
  private static final String PK_FIELDS_DOC =
      "List of comma-separated primary key field names. The runtime interpretation of this config"
      + " depends on the ``pk.mode``:\n"
      + "``none``\n"
      + "    Ignored as no fields are used as primary key in this mode.\n"
      + "``kafka``\n"
      + "    Must be a trio representing the Kafka coordinates, defaults to ``"
      + StringUtils.join(DEFAULT_KAFKA_PK_NAMES, ",") + "`` if empty.\n"
      + "``record_key``\n"
      + "    If empty, all fields from the key struct will be used, otherwise used to extract the"
      + " desired fields - for primitive key only a single field name must be configured.\n"
      + "``record_value``\n"
      + "    If empty, all fields from the value struct will be used, otherwise used to extract "
      + "the desired fields.";
  private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";

  public static final String PK_MODE = "pk.mode";
  private static final String PK_MODE_DEFAULT = "none";
  private static final String PK_MODE_DOC =
      "The primary key mode, also refer to ``" + PK_FIELDS + "`` documentation for interplay. "
      + "Supported modes are:\n"
      + "``none``\n"
      + "    No keys utilized.\n"
      + "``kafka``\n"
      + "    Kafka coordinates are used as the PK.\n"
      + "``record_key``\n"
      + "    Field(s) from the record key are used, which may be a primitive or a struct.\n"
      + "``record_value``\n"
      + "    Field(s) from the record value are used, which must be a struct.";
  private static final String PK_MODE_DISPLAY = "Primary Key Mode";

  public static final String FIELDS_WHITELIST = "fields.whitelist";
  private static final String FIELDS_WHITELIST_DEFAULT = "";
  private static final String FIELDS_WHITELIST_DOC =
      "List of comma-separated record value field names. If empty, all fields from the record "
      + "value are utilized, otherwise used to filter to the desired fields.\n"
      + "Note that ``" + PK_FIELDS + "`` is applied independently in the context of which field"
      + "(s) form the primary key columns in the destination database,"
      + " while this configuration is applicable for the other columns.";
  private static final String FIELDS_WHITELIST_DISPLAY = "Fields Whitelist";

  public static final String FIELDS_BLACKLIST = "fields.blacklist";
  private static final String FIELDS_BLACKLIST_DEFAULT = "";
  private static final String FIELDS_BLACKLIST_DOC =
      "List of comma-separated record value field names. If empty, all fields from the record "
          + "value are utilized, otherwise used to filter out the non-desired fields.\n"
          + "Note that ``" + PK_FIELDS + "`` is applied independently in the context of which field"
          + "(s) form the primary key columns in the destination database,"
          + " while this configuration is applicable for the other columns.";
  private static final String FIELDS_BLACKLIST_DISPLAY = "Fields Blacklist";

  private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

  private static final String CONNECTION_GROUP = "Connection";
  private static final String WRITES_GROUP = "Writes";
  private static final String DATAMAPPING_GROUP = "Data Mapping";
  private static final String DDL_GROUP = "DDL Support";
  private static final String RETRIES_GROUP = "Retries";

  public static final String TABLE_TYPES_CONFIG = "table.types";
  private static final String TABLE_TYPES_DISPLAY = "Table Types";
  public static final String TABLE_TYPES_DEFAULT = TableType.TABLE.toString();
  private static final String TABLE_TYPES_DOC =
      "The comma-separated types of database tables to which the sink connector can write. "
      + "By default this is ``" + TableType.TABLE + "``, but any combination of ``"
      + TableType.TABLE + "`` and ``" + TableType.VIEW + "`` is allowed. Not all databases "
      + "support writing to views, and when they do the the sink connector will fail if the "
      + "view definition does not match the records' schemas (regardless of ``"
      + AUTO_EVOLVE + "``).";

  public static final String TOPIC_NAMING_MODE_CONFIG = "topic.naming.mode";
  private static final String TOPIC_NAMING_MODE_DISPLAY = "Topic naming mode";
  private static final String TOPIC_NAMING_MODE_DEFAULT = "normal";
  private static final String TOPIC_NAMING_MODE_DOC =
      "The topic naming mode, default is normal. "
      + "Supported modes are:\n"
      + "``normal``\n"
      + "    Determined by the dialect based upon the JDBC connection URL.\n"
      + "``debezium``\n"
      + "    The topic generated by debezium component, the topic name like "
      + "`debezium_server_name.database.table`.";

  public static final String TABLE_SHARD_MAPPING_CONFIG = "table.shard.mapping";
  private static final String TABLE_SHARD_MAPPING_DISPLAY = "Table shard mapping";
  private static final String TABLE_SHARD_MAPPING_DEFAULT = "";
  private static final String TABLE_SHARD_MAPPING_DOC =
      "The table shard mapping like `topic_name:shard_column_name:shard_mode`, "
      + "and the delimiter is comma when multiple mapping configs"
      + " ``topic_name`` \n"
      + "    Specify the Topic name.\n"
      + " ``shard_column_name`` \n"
      + "    Extract the shard value from it, only support unix timestamp, like 1663672374501.\n"
      + " ``shard_mode`` \n"
      + "    Specify the shard mode, like year, month, day, hour";

  public static final String TABLE_ID_MAPPING_CONFIG = "table.id.mapping";
  private static final String TABLE_ID_MAPPING_DISPLAY = "Table id mapping";
  private static final String TABLE_ID_MAPPING_DEFAULT = "";
  private static final String TABLE_ID_MAPPING_DOC =
      "Mapping the table id from parsing topic name to specified name, "
      + "the value like: topic_database.topic_table:specified_database.specified_table,"
      + "topic_database_2.topic_table_2:specified_database_2.specified_table_2";

  public static final String TOPICS_EXCLUDE_LIST_CONFIG = "topics.exclude.list";
  private static final String TOPICS_EXCLUDE_LIST_DISPLAY = "Topics exclude list";
  private static final String TOPICS_EXCLUDE_LIST_DEFAULT = "";
  private static final String TOPICS_EXCLUDE_LIST_DOC =
      "Excluding the list of topics that are specified by the topic name with comma separated";

  public static final String UDF_COLUMN_LIST_CONFIG = "udf.column.list";
  private static final String UDF_COLUMN_LIST_DISPLAY = "Udf column list";
  private static final String UDF_COLUMN_LIST_DEFAULT = "";
  private static final String UDF_COLUMN_LIST_DOC =
      "The user define function column, like `table_name:column_name:function_name:return_type`, "
      + "and the delimiter is '|' when multiple mapping configs";

  public static final String DORIS_FE_PORT_CONFIG = "doris.fe.port";
  private static final String DORIS_FE_PORT_DISPLAY = "Doris fe port";
  private static final String DORIS_FE_PORT_DEFAULT = "8030";
  private static final String DORIS_FE_PORT_DOC = "Specify the port for the doris fe";

  private static final EnumRecommender QUOTE_METHOD_RECOMMENDER =
      EnumRecommender.in(QuoteMethod.values());

  private static final EnumRecommender TABLE_TYPES_RECOMMENDER =
      EnumRecommender.in(TableType.values());

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
        // Connection
        .define(
            CONNECTION_URL_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTION_GROUP,
            1,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            CONNECTION_USER_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            CONNECTION_USER_DOC,
            CONNECTION_GROUP,
            2,
            ConfigDef.Width.MEDIUM,
            CONNECTION_USER_DISPLAY
        )
        .define(
            CONNECTION_PASSWORD_CONFIG,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.HIGH,
            CONNECTION_PASSWORD_DOC,
            CONNECTION_GROUP,
            3,
            ConfigDef.Width.MEDIUM,
            CONNECTION_PASSWORD_DISPLAY
        )
        .define(
            DIALECT_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DIALECT_NAME_DEFAULT,
            DatabaseDialectRecommender.INSTANCE,
            ConfigDef.Importance.LOW,
            DIALECT_NAME_DOC,
            CONNECTION_GROUP,
            4,
            ConfigDef.Width.LONG,
            DIALECT_NAME_DISPLAY,
            DatabaseDialectRecommender.INSTANCE
        )
        .define(
            CONNECTION_ATTEMPTS_CONFIG,
            ConfigDef.Type.INT,
            CONNECTION_ATTEMPTS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.LOW,
            CONNECTION_ATTEMPTS_DOC,
            CONNECTION_GROUP,
            5,
            ConfigDef.Width.SHORT,
            CONNECTION_ATTEMPTS_DISPLAY
        ).define(
            CONNECTION_BACKOFF_CONFIG,
            ConfigDef.Type.LONG,
            CONNECTION_BACKOFF_DEFAULT,
            ConfigDef.Importance.LOW,
            CONNECTION_BACKOFF_DOC,
            CONNECTION_GROUP,
            6,
            ConfigDef.Width.SHORT,
            CONNECTION_BACKOFF_DISPLAY
        )
        // Writes
        .define(
            INSERT_MODE,
            ConfigDef.Type.STRING,
            INSERT_MODE_DEFAULT,
            EnumValidator.in(InsertMode.values()),
            ConfigDef.Importance.HIGH,
            INSERT_MODE_DOC,
            WRITES_GROUP,
            1,
            ConfigDef.Width.MEDIUM,
            INSERT_MODE_DISPLAY
        )
        .define(
            BATCH_SIZE,
            ConfigDef.Type.INT,
            BATCH_SIZE_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            BATCH_SIZE_DOC, WRITES_GROUP,
            2,
            ConfigDef.Width.SHORT,
            BATCH_SIZE_DISPLAY
        )
        .define(
            DELETE_ENABLED,
            ConfigDef.Type.BOOLEAN,
            DELETE_ENABLED_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            DELETE_ENABLED_DOC, WRITES_GROUP,
            3,
            ConfigDef.Width.SHORT,
            DELETE_ENABLED_DISPLAY,
            DeleteEnabledRecommender.INSTANCE
        )
        .define(
            TABLE_TYPES_CONFIG,
            ConfigDef.Type.LIST,
            TABLE_TYPES_DEFAULT,
            TABLE_TYPES_RECOMMENDER,
            ConfigDef.Importance.LOW,
            TABLE_TYPES_DOC,
            WRITES_GROUP,
            4,
            ConfigDef.Width.MEDIUM,
            TABLE_TYPES_DISPLAY
        )
        // Data Mapping
        .define(
            TABLE_NAME_FORMAT,
            ConfigDef.Type.STRING,
            TABLE_NAME_FORMAT_DEFAULT,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.MEDIUM,
            TABLE_NAME_FORMAT_DOC,
            DATAMAPPING_GROUP,
            1,
            ConfigDef.Width.LONG,
            TABLE_NAME_FORMAT_DISPLAY
        )
        .define(
            PK_MODE,
            ConfigDef.Type.STRING,
            PK_MODE_DEFAULT,
            EnumValidator.in(PrimaryKeyMode.values()),
            ConfigDef.Importance.HIGH,
            PK_MODE_DOC,
            DATAMAPPING_GROUP,
            2,
            ConfigDef.Width.MEDIUM,
            PK_MODE_DISPLAY,
            PrimaryKeyModeRecommender.INSTANCE
        )
        .define(
            PK_FIELDS,
            ConfigDef.Type.LIST,
            PK_FIELDS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            PK_FIELDS_DOC,
            DATAMAPPING_GROUP,
            3,
            ConfigDef.Width.LONG, PK_FIELDS_DISPLAY
        )
        .define(
            FIELDS_WHITELIST,
            ConfigDef.Type.LIST,
            FIELDS_WHITELIST_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FIELDS_WHITELIST_DOC,
            DATAMAPPING_GROUP,
            4,
            ConfigDef.Width.LONG,
            FIELDS_WHITELIST_DISPLAY
        )
        .define(
            FIELDS_BLACKLIST,
            ConfigDef.Type.LIST,
            FIELDS_BLACKLIST_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FIELDS_BLACKLIST_DOC,
            DATAMAPPING_GROUP,
            4,
            ConfigDef.Width.LONG,
            FIELDS_BLACKLIST_DISPLAY
        ).define(
          DB_TIMEZONE_CONFIG,
          ConfigDef.Type.STRING,
          DB_TIMEZONE_DEFAULT,
          TimeZoneValidator.INSTANCE,
          ConfigDef.Importance.MEDIUM,
          DB_TIMEZONE_CONFIG_DOC,
          DATAMAPPING_GROUP,
          5,
          ConfigDef.Width.MEDIUM,
          DB_TIMEZONE_CONFIG_DISPLAY
        )
        // DDL
        .define(
            AUTO_CREATE,
            ConfigDef.Type.BOOLEAN,
            AUTO_CREATE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            AUTO_CREATE_DOC, DDL_GROUP,
            1,
            ConfigDef.Width.SHORT,
            AUTO_CREATE_DISPLAY
        )
        .define(
            AUTO_EVOLVE,
            ConfigDef.Type.BOOLEAN,
            AUTO_EVOLVE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            AUTO_EVOLVE_DOC, DDL_GROUP,
            2,
            ConfigDef.Width.SHORT,
            AUTO_EVOLVE_DISPLAY
        ).define(
            QUOTE_SQL_IDENTIFIERS_CONFIG,
            ConfigDef.Type.STRING,
            QUOTE_SQL_IDENTIFIERS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            QUOTE_SQL_IDENTIFIERS_DOC,
            DDL_GROUP,
            3,
            ConfigDef.Width.MEDIUM,
            QUOTE_SQL_IDENTIFIERS_DISPLAY,
            QUOTE_METHOD_RECOMMENDER
        )
        .define(
            TOPIC_NAMING_MODE_CONFIG,
            ConfigDef.Type.STRING,
            TOPIC_NAMING_MODE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            TOPIC_NAMING_MODE_DOC,
            DDL_GROUP,
           4,
            ConfigDef.Width.MEDIUM,
            TOPIC_NAMING_MODE_DISPLAY
        )
        .define(
            TABLE_SHARD_MAPPING_CONFIG,
            ConfigDef.Type.STRING,
            TABLE_SHARD_MAPPING_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            TABLE_SHARD_MAPPING_DOC,
            DDL_GROUP,
            5,
            ConfigDef.Width.MEDIUM,
            TABLE_SHARD_MAPPING_DISPLAY
        )
        .define(
            TABLE_ID_MAPPING_CONFIG,
            ConfigDef.Type.STRING,
            TABLE_ID_MAPPING_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            TABLE_ID_MAPPING_DOC,
            DDL_GROUP,
            5,
            ConfigDef.Width.MEDIUM,
            TABLE_ID_MAPPING_DISPLAY
        )
        .define(
            TOPICS_EXCLUDE_LIST_CONFIG,
            ConfigDef.Type.LIST,
            TOPICS_EXCLUDE_LIST_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            TOPICS_EXCLUDE_LIST_DOC,
            DDL_GROUP,
            5,
            ConfigDef.Width.MEDIUM,
            TOPICS_EXCLUDE_LIST_DISPLAY
          )
        .define(
            UDF_COLUMN_LIST_CONFIG,
            ConfigDef.Type.STRING,
            UDF_COLUMN_LIST_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            UDF_COLUMN_LIST_DOC,
            DDL_GROUP,
            5,
            ConfigDef.Width.MEDIUM,
            UDF_COLUMN_LIST_DISPLAY
          )
        .define(
            DORIS_FE_PORT_CONFIG,
            ConfigDef.Type.INT,
            DORIS_FE_PORT_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            DORIS_FE_PORT_DOC,
            DDL_GROUP,
            5,
            ConfigDef.Width.MEDIUM,
            DORIS_FE_PORT_DISPLAY
        )
        // Retries
        .define(
            MAX_RETRIES,
            ConfigDef.Type.INT,
            MAX_RETRIES_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            MAX_RETRIES_DOC,
            RETRIES_GROUP,
            1,
            ConfigDef.Width.SHORT,
            MAX_RETRIES_DISPLAY
        )
        .define(
            RETRY_BACKOFF_MS,
            ConfigDef.Type.INT,
            RETRY_BACKOFF_MS_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            RETRY_BACKOFF_MS_DOC,
            RETRIES_GROUP,
            2,
            ConfigDef.Width.SHORT,
            RETRY_BACKOFF_MS_DISPLAY
        );

  private static final Logger log = LoggerFactory.getLogger(JdbcSinkConfig.class);
  public static final Pattern COLON_PATTERN = Pattern.compile(":");
  public final String connectionUrl;
  public final String connectionUser;
  public final String connectionPassword;
  public final int connectionAttempts;
  public final long connectionBackoffMs;
  public final String tableNameFormat;
  public final int batchSize;
  public final boolean deleteEnabled;
  public final int maxRetries;
  public final int retryBackoffMs;
  public final boolean autoCreate;
  public final boolean autoEvolve;
  public final InsertMode insertMode;
  public final PrimaryKeyMode pkMode;
  public final List<String> pkFields;
  public final Set<String> fieldsWhitelist;
  public final Set<String> fieldsBlacklist;
  public final String dialectName;
  public final TimeZone timeZone;
  public final EnumSet<TableType> tableTypes;
  public final TopicNamingMode topicNamingMode;
  public final Map<String, TableShardDefinition> tableShardDefinitions;
  public final Map<String, String> rawTableIdMapping;
  public final List<String> topicsExcludeList;
  public final List<UdfField> udfColumnList;
  public final int dorisFePort;

  public JdbcSinkConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    connectionUrl = getString(CONNECTION_URL_CONFIG);
    connectionUser = getString(CONNECTION_USER_CONFIG);
    connectionPassword = getPasswordValue(CONNECTION_PASSWORD_CONFIG);
    connectionAttempts = getInt(CONNECTION_ATTEMPTS_CONFIG);
    connectionBackoffMs = getLong(CONNECTION_BACKOFF_CONFIG);
    tableNameFormat = getString(TABLE_NAME_FORMAT).trim();
    batchSize = getInt(BATCH_SIZE);
    deleteEnabled = getBoolean(DELETE_ENABLED);
    maxRetries = getInt(MAX_RETRIES);
    retryBackoffMs = getInt(RETRY_BACKOFF_MS);
    autoCreate = getBoolean(AUTO_CREATE);
    autoEvolve = getBoolean(AUTO_EVOLVE);
    insertMode = InsertMode.valueOf(getString(INSERT_MODE).toUpperCase());
    pkMode = PrimaryKeyMode.valueOf(getString(PK_MODE).toUpperCase());
    pkFields = getList(PK_FIELDS);
    dialectName = getString(DIALECT_NAME_CONFIG);
    fieldsWhitelist = new HashSet<>(getList(FIELDS_WHITELIST));
    fieldsBlacklist = new HashSet<>(getList(FIELDS_BLACKLIST));
    String dbTimeZone = getString(DB_TIMEZONE_CONFIG);
    timeZone = TimeZone.getTimeZone(ZoneId.of(dbTimeZone));

    if (deleteEnabled && pkMode != PrimaryKeyMode.RECORD_KEY) {
      throw new ConfigException(
          "Primary key mode must be 'record_key' when delete support is enabled");
    }
    tableTypes = TableType.parse(getList(TABLE_TYPES_CONFIG));
    topicNamingMode = TopicNamingMode.valueOf(getString(TOPIC_NAMING_MODE_CONFIG).toUpperCase());
    tableShardDefinitions = TableShardDefinition.parse(getString(TABLE_SHARD_MAPPING_CONFIG));
    rawTableIdMapping = parseRawTableIdMapping();
    topicsExcludeList = getList(TOPICS_EXCLUDE_LIST_CONFIG);
    udfColumnList = parseUdfColumnList();
    dorisFePort = getInt(DORIS_FE_PORT_CONFIG);
  }

  private Map<String, String> parseRawTableIdMapping() {
    Map<String, String> rawTableIdMapping = new HashMap<>();
    String raw = getString(TABLE_ID_MAPPING_CONFIG);
    if (StringUtils.isBlank(raw)) {
      return rawTableIdMapping;
    }
    for (String mapping : raw.split(",")) {
      String[] items = mapping.trim().split(":");
      if (items.length == 2) {
        rawTableIdMapping.put(items[0].trim(), items[1].trim());
      }
    }
    return rawTableIdMapping;
  }

  private List<UdfField> parseUdfColumnList() {
    List<UdfField> udfColumnList = new ArrayList<>();
    String raw = getString(UDF_COLUMN_LIST_CONFIG);
    if (StringUtils.isBlank(raw)) {
      return udfColumnList;
    }
    for (String udf : raw.split("\\|")) {
      String[] elements = COLON_PATTERN.split(udf);
      if (elements.length == 4) {
        UdfField udfField = new UdfField(elements[0], elements[1], elements[2], elements[3]);
        udfColumnList.add(udfField);
      } else if (elements.length >= 5) {
        String[] inputColumns = new String[elements.length - 4];
        System.arraycopy(elements, 4, inputColumns, 0, inputColumns.length);
        UdfField udfField =
            new UdfField(elements[0], elements[1], elements[2], elements[3], inputColumns);
        udfColumnList.add(udfField);
      } else {
        log.warn("Parse udf field failed: {}", udf);
      }
    }
    return udfColumnList;
  }

  private String getPasswordValue(String key) {
    Password password = getPassword(key);
    if (password != null) {
      return password.value();
    }
    return null;
  }

  public EnumSet<TableType> tableTypes() {
    return tableTypes;
  }

  public Set<String> tableTypeNames() {
    return tableTypes().stream().map(TableType::toString).collect(Collectors.toSet());
  }

  @Override
  public String getContextName() {
    return "sink";
  }

  private static class EnumValidator implements ConfigDef.Validator {
    private final List<String> canonicalValues;
    private final Set<String> validValues;

    private EnumValidator(List<String> canonicalValues, Set<String> validValues) {
      this.canonicalValues = canonicalValues;
      this.validValues = validValues;
    }

    public static <E> EnumValidator in(E[] enumerators) {
      final List<String> canonicalValues = new ArrayList<>(enumerators.length);
      final Set<String> validValues = new HashSet<>(enumerators.length * 2);
      for (E e : enumerators) {
        canonicalValues.add(e.toString().toLowerCase());
        validValues.add(e.toString().toUpperCase());
        validValues.add(e.toString().toLowerCase());
      }
      return new EnumValidator(canonicalValues, validValues);
    }

    @Override
    public void ensureValid(String key, Object value) {
      if (!validValues.contains(value)) {
        throw new ConfigException(key, value, "Invalid enumerator");
      }
    }

    @Override
    public String toString() {
      return canonicalValues.toString();
    }
  }

  public static void main(String... args) {
    System.out.println(CONFIG_DEF.toEnrichedRst());
  }

}
