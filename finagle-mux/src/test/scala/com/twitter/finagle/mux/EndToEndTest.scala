package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle.{Dtab, Mux, Service}
import com.twitter.util.{Await, Future, Promise}
import java.io.{PrintWriter, StringWriter}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite {
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
    Await.ready(f, 30.seconds)
    assert(handled)
  }

  test("Dtab propagation") {
    val server = Mux.serve(":*", new Service[ChannelBuffer, ChannelBuffer] {
      def apply(req: ChannelBuffer) = {
        val stringer = new StringWriter
        val printer = new PrintWriter(stringer)
        Dtab.baseDiff().print(printer)
        Future.value(ChannelBuffers.wrappedBuffer(stringer.toString.getBytes))
      }
    })

    val client = Mux.newService(server)

    Dtab.unwind {
      Dtab.delegate("/foo", "/bar")
      Dtab.delegate("/web", "inet!twitter.com:80")
      val buf = Await.result(client(ChannelBuffers.EMPTY_BUFFER), 30.seconds)
      val bytes = new Array[Byte](buf.readableBytes())
      buf.readBytes(bytes)
      val str = new String(bytes)
      assert(str === "Dtab(2)\n\t/foo -> /bar\n\t/web -> inet!twitter.com:80\n")
    }

  }
}
