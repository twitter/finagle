package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle.{param, Dtab, Mux, Service}
import com.twitter.finagle.tracing._
import com.twitter.util.{Await, Future, Promise}
import java.io.{PrintWriter, StringWriter}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite with Eventually with BeforeAndAfter {
  var saveBase: Dtab = Dtab.empty
  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
  }


  // Tagging as flaky until CSL-794 is fixed.
  if (!sys.props.contains("SKIP_FLAKY")) test("Discard request properly sent") {
    @volatile var handled = false
    val p = Promise[ChannelBuffer]()
    p.setInterruptHandler { case t: Throwable =>
      handled = true
    }

    val server = Mux.serve(":*", new Service[ChannelBuffer, ChannelBuffer] {
      def apply(req: ChannelBuffer) = {
        p
      }
    })

    val client = Mux.newService(server)

    val f = client(ChannelBuffers.EMPTY_BUFFER)
    assert(!f.isDefined)
    assert(!p.isDefined)
    f.raise(new Exception())
    eventually { assert(handled) }
  }

  test("Dtab propagation") {
    val server = Mux.serve(":*", new Service[ChannelBuffer, ChannelBuffer] {
      def apply(req: ChannelBuffer) = {
        val stringer = new StringWriter
        val printer = new PrintWriter(stringer)
        Dtab.local.print(printer)
        Future.value(ChannelBuffers.wrappedBuffer(stringer.toString.getBytes))
      }
    })

    val client = Mux.newService(server)

    Dtab.unwind {
      Dtab.local ++= Dtab.read("/foo=>/bar; /web=>/$/inet/twitter.com/80")
      val buf = Await.result(client(ChannelBuffers.EMPTY_BUFFER), 30.seconds)
      val bytes = new Array[Byte](buf.readableBytes())
      buf.readBytes(bytes)
      val str = new String(bytes)
      assert(str === "Dtab(2)\n\t/foo => /bar\n\t/web => /$/inet/twitter.com/80\n")
    }
  }

  test("(no) Dtab propagation") {
    val server = Mux.serve(":*", new Service[ChannelBuffer, ChannelBuffer] {
      def apply(req: ChannelBuffer) = {
        val buf = ChannelBuffers.buffer(4)
        buf.writeInt(Dtab.local.size)
        Future.value(buf)
      }
    })

    val client = Mux.newService(server)

    val buf = Await.result(client(ChannelBuffers.EMPTY_BUFFER), 30.seconds)
    assert(buf.readableBytes() === 4)
    assert(buf.readInt() === 0)
  }

  def assertAnnotationsInOrder(tracer: Seq[Record], annos: Seq[Annotation]) {
    assert(tracer.collect { case Record(_, _, ann, _) if annos.contains(ann) => ann } === annos)
  }

  test("trace propagation") {
    val tracer = new BufferingTracer

    var count: Int = 0
    var client: Service[ChannelBuffer, ChannelBuffer] = null

    val server = Mux.server
      .configured(param.Tracer(tracer))
      .configured(param.Label("theServer"))
      .serve(":*", new Service[ChannelBuffer, ChannelBuffer] {
        def apply(req: ChannelBuffer) = {
          count += 1
          if (count >= 1) Future.value(req)
          else client(req)
        }
      })

    client = Mux.client
      .configured(param.Tracer(tracer))
      .configured(param.Label("theClient"))
      .newService(server)

    Await.result(client(ChannelBuffers.EMPTY_BUFFER), 30.seconds)

    assertAnnotationsInOrder(tracer.toSeq, Seq(
      Annotation.ServiceName("theClient"),
      Annotation.ClientSend(),
      Annotation.Message(ClientDispatcher.ClientEnabledTraceMessage),
      Annotation.ServiceName("theServer"),
      Annotation.ServerRecv(),
      Annotation.ServerSend(),
      Annotation.Message(ServerDispatcher.ServerEnabledTraceMessage),
      Annotation.ClientRecv()
    ))

    //throw new Exception("\n" + tracer.mkString("\n"))
  }
}
