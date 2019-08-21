package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Stack, Status}
import com.twitter.finagle.http2.RefTransport
import com.twitter.finagle.http2.SerialExecutor
import com.twitter.finagle.http2.transport.Http2UpgradingTransport.UpgradeIgnoredException
import com.twitter.finagle.netty4.transport.HasExecutor
import com.twitter.finagle.transport.{QueueTransport, SimpleTransportContext, TransportContext}
import com.twitter.util.{Await, Future, Promise}
import java.util.concurrent.Executor
import io.netty.handler.codec.http._
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class Http2UpgradingTransportTest extends FunSuite with MockitoSugar {
  class Ctx {
    val (writeq, readq) = (new AsyncQueue[Any](), new AsyncQueue[Any]())
    val transport = new QueueTransport[Any, Any](writeq, readq) {
      override val context: TransportContext = new SimpleTransportContext with HasExecutor {
        private[finagle] override val executor: Executor = new SerialExecutor
      }
    }
    val ref = new RefTransport(transport)
    val p = Promise[Option[ClientSession]]()
    val clientSession = mock[ClientSession]

    def http1Status: Status = Status.Open

    val upgradingTransport = new Http2UpgradingTransport(
      transport,
      ref,
      p,
      Stack.Params.empty,
      () => http1Status
    )
  }

  val fullRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
  val fullResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

  def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  test("upgrades properly") {
    val ctx = new Ctx
    import ctx._

    var upgradeCalled = false
    val writeF = upgradingTransport.write(fullRequest)
    assert(await(writeq.poll) == fullRequest)
    val readF = upgradingTransport.read()
    assert(!readF.isDefined)
    assert(readq.offer(Http2UpgradingTransport.UpgradeSuccessful { _ =>
      upgradeCalled = true
      // We just give it back the same old transport.
      clientSession -> transport
    }))
    assert(await(p).nonEmpty)
    assert(readq.offer(fullResponse))
    assert(await(readF) == fullResponse)
  }

  test("can reject an upgrade") {
    val ctx = new Ctx
    import ctx._

    upgradingTransport.write(fullRequest)
    assert(await(writeq.poll) == fullRequest)
    val readF = upgradingTransport.read()
    assert(!readF.isDefined)
    assert(readq.offer(Http2UpgradingTransport.UpgradeRejected))
    assert(await(p).isEmpty)
    assert(readq.offer(fullResponse))
    assert(await(readF) == fullResponse)
  }

  test("honors aborted upgrade dispatches") {
    val ctx = new Ctx
    import ctx._

    val partialRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
    val partialF = upgradingTransport.write(partialRequest)
    assert(await(writeq.poll) == partialRequest)

    val readF = upgradingTransport.read()
    assert(!readF.isDefined)
    assert(readq.offer(Http2UpgradingTransport.UpgradeAborted))
    intercept[UpgradeIgnoredException.type] {
      await(p)
    }

    assert(readq.offer(fullResponse))
    assert(await(readF) == fullResponse)
  }

  test("honors the status if the upgrade is rejected") {
    var status: Status = Status.Open

    val ctx = new Ctx {
      override def http1Status: Status = status
    }
    import ctx._

    assert(upgradingTransport.status == Status.Open)
    status = Status.Closed
    // Haven't finished upgrade yet
    assert(upgradingTransport.status == Status.Open)

    // Reject upgrade rejected
    val writeF = upgradingTransport.write(fullRequest)
    assert(await(writeq.poll) == fullRequest)
    val readF = upgradingTransport.read()
    assert(!readF.isDefined)
    assert(readq.offer(Http2UpgradingTransport.UpgradeRejected))
    assert(await(p).isEmpty)
    assert(readq.offer(fullResponse))
    assert(await(readF) == fullResponse)

    // When the upgrade is rejected we revert to the parent behavior
    assert(upgradingTransport.status == Status.Closed)
  }

  test("honors the status if the upgrade is ignored") {
    var status: Status = Status.Open

    val ctx = new Ctx {
      override def http1Status: Status = status
    }
    import ctx._

    assert(upgradingTransport.status == Status.Open)
    status = Status.Closed
    // Haven't finished upgrade yet
    assert(upgradingTransport.status == Status.Open)

    // Reject upgrade rejected
    val writeF = upgradingTransport.write(fullRequest)
    assert(await(writeq.poll) == fullRequest)
    val readF = upgradingTransport.read()
    assert(!readF.isDefined)
    assert(readq.offer(Http2UpgradingTransport.UpgradeAborted))

    intercept[UpgradeIgnoredException.type] { await(p) }
    assert(readq.offer(fullResponse))
    assert(await(readF) == fullResponse)

    // When the upgrade is aborted we revert to the parent behavior
    assert(upgradingTransport.status == Status.Closed)
  }
}
