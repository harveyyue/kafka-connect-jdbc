package io.confluent.connect.jdbc.dialect;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TidbDatabaseDialectTest extends BaseDialectTest<MySqlDatabaseDialect> {
  @Override
  protected TidbDatabaseDialect createDialect() {
    return new TidbDatabaseDialect(sourceConfigWithUrl("jdbc:mysql://something"));
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected =
        "CREATE TABLE `myTable` (\n" + "`tidb_rowid` BIGINT NOT NULL /*T![auto_rand] AUTO_RANDOM */,\n" +
            "`c1` INT NOT NULL,\n" + "`c2` BIGINT NOT NULL,\n" +
            "`c3` VARCHAR(100) NOT NULL,\n" + "`c4` TEXT NULL,\n" +
            "`c5` DATE DEFAULT '2001-03-15',\n" + "`c6` TIME(3) DEFAULT '00:00:00.000',\n" +
            "`c7` DATETIME(3) DEFAULT '2001-03-15 00:00:00.000',\n" + "`c8` DECIMAL(65, 4) NULL,\n" +
            "`c9` TINYINT DEFAULT 1,\n" +
            "PRIMARY KEY(`tidb_rowid`) /*T![clustered_index] CLUSTERED */,\n" +
            "UNIQUE KEY readonly_origin_primary_key (`c1`))";
    String sql = dialect.buildCreateTableStatement(tableId, buildSinkRecordFieldsIncludingSchemaParameters());
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildMultipleAlterStatement() {
    List<String> alteredStatements = dialect.buildAlterTable(tableId, alterFields());
    List<String> expectedStatements = Arrays.asList("ALTER TABLE `myTable` ADD `foo` INT DEFAULT 42",
        "ALTER TABLE `myTable` ADD `bar` TEXT NOT NULL");
    assertEquals(2, alteredStatements.size());
    assertEquals(alteredStatements, expectedStatements);
  }
}
