package com.twitter.finagle.thrift

import com.twitter.util.{Await, Throw, Future, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Span, Millis, Seconds}
import com.twitter.finagle.Service
import com.twitter.finagle.tracing.{Record, Trace, Annotation}
import com.twitter.test._
import org.apache.thrift.protocol.TProtocolFactory

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite with ThriftTest with Eventually {
  type Iface = B.ServiceIface
  def ifaceManifest = implicitly[ClassManifest[B.ServiceIface]]

  val processor = new B.ServiceIface {
    def add(a: Int, b: Int) = Future.exception(new AnException)
    def add_one(a: Int, b: Int) = Future.Void
    def multiply(a: Int, b: Int) = Future { a * b }
    def complex_return(someString: String) = Future {
      Trace.record("hey it's me!")
      new SomeStruct(123, Trace.id.parentId.toString)
    }
    def someway() = Future.Void
  }

  val ifaceToService = new B.Service(_, _)
  val serviceToIface = new B.ServiceToClient(_, _)

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(5, Millis)))

  testThrift("unique trace ID") { (client, tracer) =>
    Time.withCurrentTimeFrozen { tc =>
      val anException = Some(Throw(new AnException))
      val f1 = client.add(1, 2)
      eventually {
        assert(f1.poll === anException)
      }
      val idSet1 = (tracer map(_.traceId.traceId)).toSet
      tracer.clear()

      val f2 = client.add(2, 3)
      eventually {
        assert(f2.poll === anException)
      }
      val idSet2 = (tracer map(_.traceId.traceId)).toSet

      assert(idSet1 != idSet2)
    }
  }

  // TODO(John Sirois): Address and un-skip: https://jira.twitter.biz/browse/CSL-549
  if (!Option(System.getProperty("SKIP_FLAKY")).isDefined) {
    testThrift("end-to-end tracing potpourri") { (client, tracer) =>
      Time.withCurrentTimeFrozen { tc =>
        Trace.unwind {
          Trace.setId(Trace.nextId)  // set an ID so we don't use the default one
          assert(Await.result(client.multiply(10, 30)) === 300)
          assert(!tracer.isEmpty)
          val idSet = (tracer map(_.traceId)).toSet
          assert(idSet.size === 1)
          val theId = idSet.head
          assert(theId.parentId === Trace.id.spanId)
          assert(theId.traceId === Trace.id.traceId)

          // verify the traces.
          def rec(annot: Annotation) = Record(theId, Time.now, annot)
          val trace = tracer.toSeq
          val Seq(clientAddr1, clientAddr2) =
            trace collect { case Record(_, _, Annotation.ClientAddr(addr), _) => addr }
          val Seq(serverAddr1, serverAddr2) =
            trace collect { case Record(_, _, Annotation.ServerAddr(addr), _) => addr }

          assert(trace.size === 11)
          assert(trace(0) === rec(Annotation.Rpcname("thriftclient", "multiply")))
          assert(trace(1) === rec(Annotation.ClientSend()))
          assert(trace(2) === rec(Annotation.ServerAddr(serverAddr1)))
          assert(trace(3) === rec(Annotation.ClientAddr(clientAddr1)))
          assert(trace(4) === rec(Annotation.Rpcname("thriftserver", "multiply")))
          assert(trace(5) === rec(Annotation.ServerRecv()))
          assert(trace(6) === rec(Annotation.LocalAddr(serverAddr2)))
          assert(trace(7) === rec(Annotation.ServerAddr(serverAddr2)))
          assert(trace(8) === rec(Annotation.ClientAddr(clientAddr2)))
          assert(trace(9) === rec(Annotation.ServerSend()))
          assert(trace(10) === rec(Annotation.ClientRecv()))

          assert(Await.result(client.complex_return("a string")).arg_two
            === "%s".format(Trace.id.spanId.toString))

          intercept[AnException] { Await.result(client.add(1, 2)) }
          Await.result(client.add_one(1, 2))     // don't block!

          assert(Await.result(client.someway()) === null)  // don't block!
        }
      }
    }
  }

  runThriftTests()
}
