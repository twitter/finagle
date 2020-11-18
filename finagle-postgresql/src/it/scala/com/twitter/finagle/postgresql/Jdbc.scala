package com.twitter.finagle.postgresql

import java.sql.DriverManager

trait Jdbc { _: PostgresConnectionSpec with PgSqlSpec =>

  def randomTableName = util.Random.alphanumeric.filter(c => c >= 'a' && c <= 'z').take(10).mkString

  def withConnection[T](cfg: ConnectionCfg = defaultConnectionCfg)(f: java.sql.Connection => T) =
    using(DriverManager.getConnection(cfg.jdbcUrl, cfg.username, cfg.password.orNull))(f)

  def withStatement[T](cfg: ConnectionCfg = defaultConnectionCfg)(f: java.sql.Statement => T) =
    withConnection(cfg) { conn =>
      using(conn.createStatement())(f)
    }

  def withTmpTable[T](cfg: ConnectionCfg = defaultConnectionCfg)(f: String => T) =
    withStatement(cfg) { stmt =>
      val tableName = randomTableName
      stmt.execute(s"CREATE TABLE $tableName(int4_col int4)") // TODO: allow specifying the table spec
      f(tableName)
    }
}
