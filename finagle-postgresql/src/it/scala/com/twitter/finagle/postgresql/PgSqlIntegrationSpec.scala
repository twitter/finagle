package com.twitter.finagle.postgresql

case class ConnectionCfg(
  username: String = "postgres",
  password: Option[String] = None,
  database: String = "postgres",
  port: Int = 5432,
  host: String = "localhost",
) {
  val jdbcUrl = s"jdbc:postgresql://$host:$port/$database"
}

trait PostgresConnectionSpec {
  def defaultConnectionCfg: ConnectionCfg = ConnectionCfg()
}

trait PgSqlIntegrationSpec
    extends PgSqlSpec
    with PostgresConnectionSpec
    with PostgresBackendService
    with ClientEach
    with Jdbc {

  override def defaultConnectionCfg = ConnectionCfg(
    username = testBackend.user,
    password = testBackend.password,
    port = container.mappedPort(testBackend.advertisedPort)
  )
}
