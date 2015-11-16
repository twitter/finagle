package com.twitter.finagle.exp.mysql.integration

import com.twitter.finagle.exp.Mysql
import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.param
import com.twitter.finagle.tracing._
import com.twitter.util.Await
import com.twitter.util.Local
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MysqlBuilderTest extends FunSuite with IntegrationClient {
  test("clients have granular tracing") {
    Trace.enable()
    var annotations: List[Annotation] = Nil
    val mockTracer = new Tracer {
      def record(record: Record) = {
        annotations ::= record.annotation
      }
      def sampleTrace(traceId: TraceId): Option[Boolean] = Some(true)
    }

    // if we have a local instance of mysql running.
    if (isAvailable) {
      val client = Mysql.client
        .configured(param.Label("myclient"))
        .configured(param.Tracer(mockTracer))
        .withDatabase("test")
        .newRichClient("localhost:3306")

      Await.ready(client.query("query"))
      Await.ready(client.prepare("prepare query")(1))
      Await.ready(client.ping())

      val mysqlTraces = annotations collect {
        case Annotation.BinaryAnnotation("mysql.query", "query") => ()
        case Annotation.BinaryAnnotation("mysql.prepare", "prepare query") => ()
        case Annotation.Message("mysql.PingRequest") => ()
      }

      assert(mysqlTraces.size == 3, "missing traces")
    }

  }
}
