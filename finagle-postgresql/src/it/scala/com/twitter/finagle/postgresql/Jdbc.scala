package com.twitter.finagle.postgresql

import java.sql.DriverManager

import com.twitter.finagle.postgresql.types.PgType

trait Jdbc { _: PostgresConnectionSpec with PgSqlSpec =>

  case class Schema(cols: Seq[(String, PgType)])
  object Schema {
    def apply(col: (String, PgType), cols: (String, PgType)*): Schema =
      Schema(col :: cols.toList)
  }
  val DefaultSchema: Schema = Schema("int4_col" -> PgType.Int4)

  def randomTableName: String = util.Random.alphanumeric.filter(c => c >= 'a' && c <= 'z').take(10).mkString

  def withConnection[T](cfg: ConnectionCfg = defaultConnectionCfg)(f: java.sql.Connection => T): T =
    using(DriverManager.getConnection(cfg.jdbcUrl, cfg.username, cfg.password.orNull))(f)

  def withStatement[T](cfg: ConnectionCfg = defaultConnectionCfg)(f: java.sql.Statement => T): T =
    withConnection(cfg) { conn =>
      using(conn.createStatement())(f)
    }

  def withTmpTable[T](cfg: ConnectionCfg = defaultConnectionCfg, schema: Schema = DefaultSchema)(f: String => T): T =
    withStatement(cfg) { stmt =>
      val tableName = randomTableName
      val cols = schema.cols.map { case (name, tpe) => s"$name ${tpe.name}" }.mkString(",")
      stmt.execute(s"CREATE TABLE $tableName($cols)")
      f(tableName)
    }
}
