package com.twitter.finagle.thrift

import com.twitter.util.{Await, Throw, Future, Time}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.time.{Span, Millis, Seconds}
import com.twitter.finagle._
import com.twitter.finagle.tracing.{Record, Trace, Annotation}
import com.twitter.test._
import java.io.{StringWriter, PrintWriter}

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite
  with ThriftTest
  with Eventually
  with IntegrationPatience
  with BeforeAndAfter {

  var saveBase: Dtab = Dtab.empty
  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
  }

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
    def show_me_your_dtab() = Future {
      val stringer = new StringWriter
      val printer = new PrintWriter(stringer)
      Dtab.local.print(printer)
      stringer.toString
    }

    def show_me_your_dtab_size() = Future {
      Dtab.local.length
    }
  }

  val ifaceToService = new B.Service(_, _)
  val serviceToIface = new B.ServiceToClient(_, _)

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

      assert(idSet1.nonEmpty)
      assert(idSet2.nonEmpty)

      assert(idSet1 != idSet2)
    }
  }

  skipTestThrift("propagate Dtab") { (client, tracer) =>
    Dtab.unwind {
      Dtab.local = Dtab.read("/a=>/b; /b=>/$/inet/google.com/80")
      val clientDtab = Await.result(client.show_me_your_dtab())
      assert(clientDtab === "Dtab(2)\n\t/a => /b\n\t/b => /$/inet/google.com/80\n")
    }
  }

  testThrift("(don't) propagate Dtab") { (client, tracer) =>
    val dtabSize = Await.result(client.show_me_your_dtab_size())
    assert(dtabSize === 0)
  }

  test("JSON is broken (before we upgrade)") {
    // We test for the presence of a JSON encoding
    // bug in thrift 0.5.0[1]. See THRIFT-1375.
    //  When we upgrade, this test will fail and helpfully
    // remind us to add JSON back.
    import org.apache.thrift.protocol._
    import org.apache.thrift.transport._
    import java.nio.ByteBuffer

    val bytes = Array[Byte](102, 100, 125, -96, 57, -55, -72, 18,
      -21, 15, -91, -36, 104, 111, 111, -127, -21, 15, -91, -36,
      104, 111, 111, -127, 0, 0, 0, 0, 0, 0, 0, 0)
    val pf = new TJSONProtocol.Factory()

    val json = {
      val buf = new TMemoryBuffer(512)
      pf.getProtocol(buf).writeBinary(ByteBuffer.wrap(bytes))
      java.util.Arrays.copyOfRange(buf.getArray(), 0, buf.length())
    }

    val decoded = {
      val trans = new TMemoryInputTransport(json)
      val bin = pf.getProtocol(trans).readBinary()
      val bytes = new Array[Byte](bin.remaining())
      bin.get(bytes, 0, bin.remaining())
      bytes
    }

    assert(bytes.toSeq != decoded.toSeq, "Add JSON support back")
  }

  skipTestThrift("end-to-end tracing potpourri") { (client, tracer) =>
    Trace.unwind {
      val id = Trace.nextId
      Trace.setId(id)  // set an ID so we don't use the default one
      assert(Await.result(client.multiply(10, 30)) === 300)

      assert(!tracer.isEmpty)
      val idSet = tracer.map(_.traceId).toSet

      val ids = idSet.filter(_.traceId == id.traceId)
      assert(ids.size === 1)
      val theId = ids.head

      // Compare two annotation records, ignoring time.
      // This is simpler than trying to freeze time in the listening threads of
      // the various servers we are constructing.
      def assertAnn(rec: Record, ann: Annotation) {
        assert(rec.annotation === ann)
      }

      val trace = tracer
        .filter(_.traceId == theId)
        .filter {
          // Skip spurious GC messages
          case Record(_, _, Annotation.Message(msg), _) => !msg.startsWith("Gc")
          case _ => true
        }
        .toSeq

      val Seq(clientAddr1, clientAddr2) =
        trace collect { case Record(_, _, Annotation.ClientAddr(addr), _) => addr }
      val Seq(serverAddr1, serverAddr2) =
        trace collect { case Record(_, _, Annotation.ServerAddr(addr), _) => addr }

      // verify the count and ordering of the annotations
      var i = 0
      def nextTrace = { i += 1; trace(i-1) }

      nextTrace // finagle.version
      assertAnn(nextTrace, Annotation.ServiceName("thriftclient"))
      assertAnn(nextTrace, Annotation.ClientSend())
      assertAnn(nextTrace, Annotation.Rpc("multiply"))
      assertAnn(nextTrace, Annotation.ServerAddr(serverAddr1))
      assertAnn(nextTrace, Annotation.ClientAddr(clientAddr1))
      assertAnn(nextTrace, Annotation.Rpc("multiply"))
      nextTrace // finagle.version
      assertAnn(nextTrace, Annotation.ServiceName("thriftserver"))
      assertAnn(nextTrace, Annotation.ServerRecv())
      assertAnn(nextTrace, Annotation.LocalAddr(serverAddr2))
      assertAnn(nextTrace, Annotation.ServerAddr(serverAddr2))
      assertAnn(nextTrace, Annotation.ClientAddr(clientAddr2))
      assertAnn(nextTrace, Annotation.ServerSend())
      assertAnn(nextTrace, Annotation.ClientRecv())

      assert(trace.size === i)


      assert(Await.result(client.complex_return("a string")).arg_two
        === "%s".format(Trace.id.spanId.toString))

      intercept[AnException] { Await.result(client.add(1, 2)) }
      Await.result(client.add_one(1, 2))     // don't block!

      assert(Await.result(client.someway()) === null)  // don't block!
    }
  }

  runThriftTests()
}

/*

[1]
% diff -u /Users/marius/src/thrift-0.5.0-finagle/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java /Users/marius/pkg/thrift/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java
--- /Users/marius/src/thrift-0.5.0-finagle/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java	2013-09-16 12:17:53.000000000 -0700
+++ /Users/marius/pkg/thrift/lib/java/src/org/apache/thrift/protocol/TJSONProtocol.java	2013-09-05 20:20:07.000000000 -0700
@@ -313,7 +313,7 @@
   // Temporary buffer used by several methods
   private byte[] tmpbuf_ = new byte[4];

-  // Read a byte that must match b[0]; otherwise an excpetion is thrown.
+  // Read a byte that must match b[0]; otherwise an exception is thrown.
   // Marked protected to avoid synthetic accessor in JSONListContext.read
   // and JSONPairContext.read
   protected void readJSONSyntaxChar(byte[] b) throws TException {
@@ -331,7 +331,7 @@
       return (byte)((char)ch - '0');
     }
     else if ((ch >= 'a') && (ch <= 'f')) {
-      return (byte)((char)ch - 'a');
+      return (byte)((char)ch - 'a' + 10);
     }
     else {
       throw new TProtocolException(TProtocolException.INVALID_DATA,
@@ -346,7 +346,7 @@
       return (byte)((char)val + '0');
     }
     else {
-      return (byte)((char)val + 'a');
+      return (byte)((char)(val - 10) + 'a');
     }
   }

*/
