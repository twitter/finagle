package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.mux.Message._
import com.twitter.finagle.mux.lease.exp.{Lessee, Lessor}
import com.twitter.finagle.netty3.{ChannelBufferBuf, BufChannelBuffer}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.tracing._
import com.twitter.io.Buf
import com.twitter.util.{Await, Future, Promise, Duration, Closable, Time}
import java.io.{PrintWriter, StringWriter}
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalatest.concurrent.{IntegrationPatience, Eventually}
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite
  with Eventually with IntegrationPatience with BeforeAndAfter with AssertionsForJUnit {

  var saveBase: Dtab = Dtab.empty
  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
  }

  test("Discard request properly sent") {
    @volatile var handled = false
    val p = Promise[Response]()
    p.setInterruptHandler { case t: Throwable =>
      handled = true
    }

    val server = Mux.serve(":*", Service.mk[Request, Response](_ => p))

    val client = Mux.newService(server)

    val f = client(Request(Path.empty, Buf.Empty))
    assert(!f.isDefined)
    assert(!p.isDefined)
    f.raise(new Exception())
    eventually { assert(handled) }
  }

  test("Dtab propagation") {
    val server = Mux.serve(":*", Service.mk[Request, Response] { _ =>
      val stringer = new StringWriter
      val printer = new PrintWriter(stringer)
      Dtab.local.print(printer)
      Future.value(Response(Buf.Utf8(stringer.toString)))
    })

    val client = Mux.newService(server)

    Dtab.unwind {
      Dtab.local ++= Dtab.read("/foo=>/bar; /web=>/$/inet/twitter.com/80")
      for (n <- 0 until 2) {
        val rsp = Await.result(client(Request(Path.empty, Buf.Empty)), 30.seconds)
        val Buf.Utf8(str) = rsp.body
        assert(str === "Dtab(2)\n\t/foo => /bar\n\t/web => /$/inet/twitter.com/80\n")
      }
    }
  }

  test("(no) Dtab propagation") {
    val server = Mux.serve(":*", Service.mk[Request, Response] { _ =>
      val buf = ChannelBuffers.buffer(4)
      buf.writeInt(Dtab.local.size)
      Future.value(Response(ChannelBufferBuf.Owned(buf)))
    })

    val client = Mux.newService(server)

    val payload = Await.result(client(Request.empty), 30.seconds).body
    val cb = BufChannelBuffer(payload)
    assert(cb.readableBytes() === 4)
    assert(cb.readInt() === 0)
  }

  def assertAnnotationsInOrder(tracer: Seq[Record], annos: Seq[Annotation]) {
    assert(tracer.collect { case Record(_, _, ann, _) if annos.contains(ann) => ann } === annos)
  }

  test("trace propagation") {
    val tracer = new BufferingTracer

    var count: Int = 0
    var client: Service[Request, Response] = null

    val server = Mux.server
      .configured(param.Tracer(tracer))
      .configured(param.Label("theServer"))
      .serve(":*", new Service[Request, Response] {
        def apply(req: Request) = {
          count += 1
          if (count >= 1) Future.value(Response(req.body))
          else client(req)
        }
      })

    client = Mux.client
      .configured(param.Tracer(tracer))
      .configured(param.Label("theClient"))
      .newService(server)

    Await.result(client(Request.empty), 30.seconds)

    assertAnnotationsInOrder(tracer.toSeq, Seq(
      Annotation.ServiceName("theClient"),
      Annotation.ClientSend(),
      Annotation.BinaryAnnotation("clnt/mux/enabled", true),
      Annotation.ServiceName("theServer"),
      Annotation.ServerRecv(),
      Annotation.BinaryAnnotation("srv/mux/enabled", true),
      Annotation.ServerSend(),
      Annotation.ClientRecv()
    ))
  }

  test("responds to lease") {
    Time.withCurrentTimeFrozen { ctl =>
      class FakeLessor extends Lessor {
        var list: List[Lessee] = Nil

        def register(lessee: Lessee): Unit = {
          list ::= lessee
        }

        def unregister(lessee: Lessee): Unit = ()

        def observe(d: Duration): Unit = ()

        def observeArrival(): Unit = ()
      }
      val lessor = new FakeLessor

      val server = Mux.server
        .configured(Lessor.Param(lessor))
        .serve(":*", new Service[mux.Request, mux.Response] {
          def apply(req: Request) = ???
        }
      )

      val sr = new InMemoryStatsReceiver

      val factory = Mux.client.configured(param.Stats(sr)).newClient(server)
      val fclient = factory()
      eventually { assert(fclient.isDefined) }

      val Some((_, available)) = sr.gauges.find {
        case (_ +: Seq("loadbalancer", "available"), value) => true
        case _ => false
      }

      val Some((_, leaseDuration)) = sr.gauges.find {
        case (_ +: Seq("current_lease_ms"), value) => true
        case _ => false
      }

      val leaseCtr: () => Int = { () =>
        val Some((_, ctr)) = sr.counters.find {
          case (_ +: Seq("leased"), value) => true
          case _ => false
        }
        ctr
      }
      def format(duration: Duration): Float = duration.inMilliseconds.toFloat

      eventually { assert(leaseDuration() === format(Time.Top - Time.now)) }
      eventually { assert(available() === 1) }
      lessor.list.foreach(_.issue(Tlease.MinLease))
      eventually { assert(leaseCtr() === 1) }
      ctl.advance(2.seconds) // must advance time to re-lease and expire
      eventually { assert(leaseDuration() === format(Tlease.MinLease - 2.seconds)) }
      eventually { assert(available() === 0) }
      lessor.list.foreach(_.issue(Tlease.MaxLease))
      eventually { assert(leaseCtr() === 2) }
      eventually { assert(leaseDuration() === format(Tlease.MaxLease)) }
      eventually { assert(available() === 1) }

      Closable.sequence(Await.result(fclient), server, factory).close()
    }
  }
}
