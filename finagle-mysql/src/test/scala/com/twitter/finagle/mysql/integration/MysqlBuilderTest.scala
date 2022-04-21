package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.QueryRequest
import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.DatabaseConfig
import com.twitter.finagle.mysql.harness.config.InstanceConfig
import com.twitter.finagle.tracing._
import com.twitter.finagle.param

class MysqlBuilderTest extends EmbeddedSuite {
  val instanceConfig: InstanceConfig = defaultInstanceConfig
  val databaseConfig: DatabaseConfig = defaultDatabaseConfig

  test("clients have granular tracing") { fixture =>
    Trace.enable()
    var annotations: List[Annotation] = Nil
    val mockTracer = new Tracer {
      def record(record: Record): Unit = {
        annotations ::= record.annotation
      }
      def sampleTrace(traceId: TraceId): Option[Boolean] = Some(true)
    }
    val client = fixture
      .newClient()
      .configured(param.Label("myclient"))
      .configured(param.Tracer(mockTracer))
      .withConnectionInitRequest(
        QueryRequest("SET SESSION sql_mode='TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY'"))
      .newRichClient(fixture.instance.dest)

    await(client.query("CREATE TABLE FOO(ID INT)"))
    await(client.query("CREATE TABLE BOO(ID INT)"))
    await(client.query("SELECT 1 FROM FOO"))
    await(client.prepare("SELECT ? FROM BOO")(1))
    await(client.query("DROP TABLE FOO"))
    await(client.query("DROP TABLE BOO"))
    await(client.ping())

    val mysqlTraces = annotations.collect {
      case a @ Annotation.BinaryAnnotation("clnt/mysql.query", "CREATETABLE") => a // 2
      case a @ Annotation.BinaryAnnotation("clnt/mysql.query", "SELECT") => a // 1
      case a @ Annotation.BinaryAnnotation("clnt/mysql.query", "DROP") => a // 2
      case a @ Annotation.BinaryAnnotation("clnt/mysql.prepare", "SELECT") => a // 1
      case a @ Annotation.BinaryAnnotation("clnt/mysql.query.tables", "FOO") => a // 3
      case a @ Annotation.BinaryAnnotation("clnt/mysql.query.tables", "BOO") => a // 2
      case a @ Annotation.BinaryAnnotation("clnt/mysql.prepare.tables", "BOO") => a // 1
      case a @ Annotation.Message("clnt/mysql.PingRequest") => a // 1
    }

    assert(mysqlTraces.size == 13, "missing traces")
    await(client.close())
  }
}
