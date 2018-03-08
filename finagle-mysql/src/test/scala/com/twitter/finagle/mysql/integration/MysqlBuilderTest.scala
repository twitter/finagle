package com.twitter.finagle.mysql.integration

import com.twitter.finagle.Mysql
import com.twitter.finagle.param
import com.twitter.finagle.tracing._
import com.twitter.util.Await
import org.scalatest.FunSuite

class MysqlBuilderTest extends FunSuite with IntegrationClient {
  test("clients have granular tracing") {
    Trace.enable()
    var annotations: List[Annotation] = Nil
    val mockTracer = new Tracer {
      def record(record: Record): Unit = {
        annotations ::= record.annotation
      }
      def sampleTrace(traceId: TraceId): Option[Boolean] = Some(true)
    }

    // if we have a local instance of mysql running.
    if (isAvailable) {
      val username = p.getProperty("username", "<user>")
      val password = p.getProperty("password", null)
      val db = p.getProperty("db", "test")
      val client = Mysql.client
        .configured(param.Label("myclient"))
        .configured(param.Tracer(mockTracer))
        .withDatabase("test")
        .withCredentials(username, password)
        .withDatabase(db)
        .newRichClient("localhost:3306")

      Await.ready(client.query("SELECT 1"))
      Await.ready(client.prepare("SELECT ?")(1))
      Await.ready(client.ping())

      val mysqlTraces = annotations.collect {
        case Annotation.BinaryAnnotation("mysql.query", "SELECT 1") => ()
        case Annotation.BinaryAnnotation("mysql.prepare", "SELECT ?") => ()
        case Annotation.Message("mysql.PingRequest") => ()
      }

      assert(mysqlTraces.nonEmpty, "missing traces")
    }

  }
}
