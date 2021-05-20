package com.twitter.finagle.http2.transport.client

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.{Stack, Status}
import com.twitter.util.{Await, Future}
import io.netty.handler.codec.http._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class Http2UpgradingTransportTest extends AnyFunSuite with MockitoSugar {
  class Ctx {
    val (writeq, readq) = (new AsyncQueue[Any](), new AsyncQueue[Any]())
    val transport = new QueueTransport[Any, Any](writeq, readq)

    val ref = new RefTransport(transport)

    def http1Status: Status = Status.Open

    val upgradingTransport = new Http2UpgradingTransport(
      transport,
      ref,
      Stack.Params.empty
    )
  }

  val fullRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
  val fullResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

  def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  test("upgrades properly") {
    val ctx = new Ctx
    import ctx._

    upgradingTransport.write(fullRequest)
    assert(await(writeq.poll) == fullRequest)

    val readF = upgradingTransport.read()
    assert(!readF.isDefined)

    val newReadQueue = new AsyncQueue[Any]()
    val newTransport = new QueueTransport[Any, Any](writeq, newReadQueue)

    assert(readq.offer(Http2UpgradingTransport.UpgradeSuccessful(newTransport)))
    assert(!readF.isDefined)

    assert(newReadQueue.offer(fullResponse))
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
    assert(!readF.isDefined)

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

    assert(!readF.isDefined)

    assert(readq.offer(fullResponse))
    assert(await(readF) == fullResponse)
  }
}
