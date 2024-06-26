package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.harness.config.DatabaseConfig
import com.twitter.finagle.mysql.harness.config.InstanceConfig
import com.twitter.finagle.mysql.harness.config.MySqlVersion

package object integration {

  val v5_7_28 = MySqlVersion(
    5,
    7,
    28,
    Map(
      "--innodb_use_native_aio" -> "0",
      "--innodb_stats_persistent" -> "0",
      "--innodb_strict_mode" -> "1",
      "--explicit_defaults_for_timestamp" -> "0",
      "--sql_mode" -> "NO_ENGINE_SUBSTITUTION",
      "--character_set_server" -> "utf8",
      "--default_time_zone" -> "+0:00",
      "--innodb_file_format" -> "Barracuda",
      "--enforce_gtid_consistency" -> "ON",
      "--log-bin" -> "binfile",
      "--server-id" -> "1"
    )
  )

  val v8_0_21: MySqlVersion = MySqlVersion(
    8,
    0,
    21,
    Map(
      "--default_storage_engine" -> "InnoDB",
      "--character_set_server" -> "utf8",
      "--collation_server" -> "utf8_general_ci",
      "--auto_increment_increment" -> "1",
      "--auto_increment_offset" -> "2",
      "--transaction_isolation" -> "REPEATABLE-READ",
      "--innodb_autoinc_lock_mode" -> "1",
      "--group_concat_max_len" -> "1024",
      "--explicit_defaults_for_timestamp" -> "0",
      "--gtid_mode" -> "OFF",
      "--enforce-gtid-consistency" -> "WARN",
      "--sql_mode" -> "NO_ENGINE_SUBSTITUTION",
      "--default_time_zone" -> "+0:00",
      "--log-bin" -> "binfile",
      "--server-id" -> "1",
      "--mysqlx" -> "OFF"
    )
  )

  /**
   * This is the default InstanceConfig used by the integration package.
   * This will look for mysql binaries in java.io.tmpdir.
   *
   * When updating this, please update the README in this folder to specify the expected
   * installation location.
   */
  val defaultInstanceConfig: InstanceConfig = InstanceConfig(v8_0_21)
  val defaultDatabaseConfig: DatabaseConfig =
    DatabaseConfig(databaseName = "a_database", users = Seq.empty, setupQueries = Seq.empty)
}
