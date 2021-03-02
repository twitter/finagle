package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig, MySqlVersion}

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

  /**
   * This is the default InstanceConfig used by the integration package.
   * This will look for mysql binaries in java.io.tmpdir.
   */
  val defaultInstanceConfig: InstanceConfig = InstanceConfig(v5_7_28)
  val defaultDatabaseConfig: DatabaseConfig =
    DatabaseConfig(databaseName = "a_database", users = Seq.empty, setupQueries = Seq.empty)
}
