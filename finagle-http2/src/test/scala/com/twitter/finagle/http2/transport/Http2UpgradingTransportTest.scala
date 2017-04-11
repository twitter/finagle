package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.Stack
import com.twitter.finagle.http2.RefTransport
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.{Promise, Future, Await}
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http._
import org.scalatest.FunSuite

class Http2UpgradingTransportTest extends FunSuite {
  class Ctx {
    val (writeq, readq) = (new AsyncQueue[Any](), new AsyncQueue[Any]())
    val transport = new QueueTransport[Any, Any](writeq, readq)
    val ref = new RefTransport(transport)
    val p = Promise[Option[MultiplexedTransporter]]()

    val upgradingTransport = new Http2UpgradingTransport(
      transport,
      ref,
      p,
      Stack.Params.empty
    )
  }

  val fullRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
  val partialRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
  val fullResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)

  def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  test("Http2UpgradingTransport upgrades properly") {
    val ctx = new Ctx
    import ctx._

    val writeF = upgradingTransport.write(fullRequest)
    assert(await(writeq.poll) == fullRequest)
    val readF = upgradingTransport.read()
    assert(!readF.isDefined)
    assert(readq.offer(UpgradeEvent.UPGRADE_SUCCESSFUL))
    assert(await(p).nonEmpty)
    assert(readq.offer(Http2ClientDowngrader.Message(fullResponse, 1)))
    assert(await(readF) == fullResponse)
  }

  test("Http2UpgradingTransport can reject an upgrade") {
    val ctx = new Ctx
    import ctx._

    val writeF = upgradingTransport.write(fullRequest)
    assert(await(writeq.poll) == fullRequest)
    val readF = upgradingTransport.read()
    assert(!readF.isDefined)
    assert(readq.offer(UpgradeEvent.UPGRADE_REJECTED))
    assert(await(p).isEmpty)
    assert(readq.offer(fullResponse))
    assert(await(readF) == fullResponse)
  }

  test("Http2UpgradingTransport delays the upgrade until the write finishes when successful") {
    val ctx = new Ctx
    import ctx._

    val partialF = upgradingTransport.write(partialRequest)
    assert(await(writeq.poll) == partialRequest)

    val readF = upgradingTransport.read()
    assert(!readF.isDefined)
    assert(readq.offer(UpgradeEvent.UPGRADE_SUCCESSFUL))
    assert(readq.offer(Http2ClientDowngrader.Message(fullResponse, 1)))
    assert(!readF.isDefined)

    val lastF = upgradingTransport.write(LastHttpContent.EMPTY_LAST_CONTENT)
    assert(await(writeq.poll) == LastHttpContent.EMPTY_LAST_CONTENT)
    assert(await(p).nonEmpty)
    assert(await(readF) == fullResponse)
  }

  test("Http2UpgradingTransport doesn't delay the upgrade until the write finishes when rejected") {
    val ctx = new Ctx
    import ctx._

    val partialF = upgradingTransport.write(partialRequest)
    assert(await(writeq.poll) == partialRequest)

    val readF = upgradingTransport.read()
    assert(!readF.isDefined)
    assert(readq.offer(UpgradeEvent.UPGRADE_REJECTED))
    assert(readq.offer(fullResponse))
    assert(await(readF) == fullResponse)
  }
}
