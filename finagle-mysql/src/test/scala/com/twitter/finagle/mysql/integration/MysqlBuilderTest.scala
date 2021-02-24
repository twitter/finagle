package com.twitter.finagle.mysql.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.mysql.QueryRequest
import com.twitter.finagle.mysql.harness.EmbeddedMySqlSuite
import com.twitter.finagle.mysql.harness.config.{MySqlDatabaseConfig, MySqlInstanceConfig}
import com.twitter.finagle.tracing._
import com.twitter.finagle.{Mysql, param}
import com.twitter.util.{Await, Awaitable}

class MysqlBuilderTest extends EmbeddedMySqlSuite {
  val mySqlInstanceConfig: MySqlInstanceConfig = defaultInstanceConfig

  val mySqlDatabaseConfig: MySqlDatabaseConfig = defaultDatabaseConfig

  private[this] def ready[T](t: Awaitable[T]): Unit = Await.ready(t, 5.seconds)

  test("clients have granular tracing") { fixture =>
    Trace.enable()
    var annotations: List[Annotation] = Nil

    // if we have a local instance of mysql running.
    val username = mySqlDatabaseConfig.users.rwUser.name
    val password = mySqlDatabaseConfig.users.rwUser.password.get
    val db = mySqlDatabaseConfig.databaseName
    val client = Mysql.client
      .configured(param.Label("myclient"))
      .withDatabase("test")
      .withCredentials(username, password)
      .withDatabase(db)
      .withConnectionInitRequest(
        QueryRequest("SET SESSION sql_mode='TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY'"))
      .newRichClient(fixture.mySqlInstance.dest)

    ready(client.query("SELECT 1"))
    ready(client.prepare("SELECT ?")(1))
    ready(client.ping())

    val mysqlTraces = annotations.collect {
      case Annotation.BinaryAnnotation("clnt/mysql.query", "SELECT") => ()
      case Annotation.BinaryAnnotation("clnt/mysql.prepare", "SELECT") => ()
      case Annotation.Message("clnt/mysql.PingRequest") => ()
    }

    assert(mysqlTraces.nonEmpty, "missing traces")
  }
}
