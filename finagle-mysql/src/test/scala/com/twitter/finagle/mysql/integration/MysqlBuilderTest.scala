package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.QueryRequest
import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig}
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

    await(client.query("SELECT 1"))
    await(client.prepare("SELECT ?")(1))
    await(client.ping())

    val mysqlTraces = annotations.collect {
      case Annotation.BinaryAnnotation("clnt/mysql.query", "SELECT") => ()
      case Annotation.BinaryAnnotation("clnt/mysql.prepare", "SELECT") => ()
      case Annotation.Message("clnt/mysql.PingRequest") => ()
    }

    assert(mysqlTraces.nonEmpty, "missing traces")
    await(client.close())
  }
}
