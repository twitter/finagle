package com.twitter.finagle.http.codec

import com.twitter.finagle.http._
import com.twitter.finagle.http.exp.IdentityStreamTransport
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Promise, Return, Future, Time}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ConnectionManagerTest extends FunSuite with MockitoSugar {
  // > further tests
  //   - malformed requests/responses
  //   - methods other than GET
  //   - 100/continue

  def makeRequest(version: Version, headers: (String, String)*) = {
    val request = Request(version, Method.Get, "/")
    headers.foreach { case (k, v) =>
      request.headerMap.set(k, v)
    }
    request
  }

  def makeResponse(version: Version, headers: (String, String)*) = {
    val response = Response(version, Status.Ok)
    headers.foreach { case (k, v) =>
      response.headerMap.set(k, v)
    }
    response
  }

  test("shouldClose returns false when initialized") {
    val manager = new ConnectionManager()
    assert(!manager.shouldClose())
  }

  test("not terminate when response is standard") {
    val manager = new ConnectionManager()
    manager.observeRequest(makeRequest(Version.Http11), Future.Done)
    assert(!manager.shouldClose())
    val rep = makeResponse(Version.Http11, Fields.ContentLength -> "1")
    manager.observeResponse(rep, Future.Done)
    assert(rep.headerMap.get(Fields.Connection) != Some("close"))
    assert(!manager.shouldClose())
  }

  test("terminate when response doesn't have content length") {
    val manager = new ConnectionManager()
    manager.observeRequest(makeRequest(Version.Http11), Future.Done)
    assert(!manager.shouldClose())
    val rep = makeResponse(Version.Http11)
    manager.observeResponse(rep, Future.Done)
    assert(manager.shouldClose())
    assert(rep.headerMap.get(Fields.Connection) == Some("close"))
  }

  test("terminate when request has Connection: close") {
    val manager = new ConnectionManager()
    manager.observeRequest(makeRequest(Version.Http11, "Connection" -> "close"), Future.Done)
    assert(!manager.shouldClose())
    val rep = makeResponse(Version.Http11, Fields.ContentLength -> "1")
    manager.observeResponse(rep, Future.Done)
    assert(manager.shouldClose())
    // the header is copied to the response
    assert(rep.headerMap.get(Fields.Connection) == Some("close"))
  }

  test("terminate after streaming request has Connection: close") {
    val manager = new ConnectionManager()

    val req = makeRequest(Version.Http11, "Connection" -> "close")
    req.setChunked(true)
    val reqP = new Promise[Unit]
    manager.observeRequest(req, reqP)
    reqP.setDone()
    assert(!manager.shouldClose())

    val rep = makeResponse(Version.Http11, "Connection" -> "close")
    rep.setChunked(true)
    val repP = new Promise[Unit]
    manager.observeResponse(rep, repP)
    assert(!manager.shouldClose())
    repP.setDone()
    assert(manager.shouldClose())
  }

  test("terminate after response, even if request hasn't finished streaming") {
    val manager = new ConnectionManager()
    val p = Promise[Unit]
    val req = makeRequest(Version.Http11)
    req.setChunked(true)
    manager.observeRequest(req, p)
    assert(!manager.shouldClose())
    manager.observeResponse(
      makeResponse(Version.Http11, Fields.ContentLength -> "1", "Connection" -> "close"),
      Future.Done)
    assert(manager.shouldClose())
  }

  test("terminate after response has finished streaming") {
    val manager = new ConnectionManager()
    manager.observeRequest(makeRequest(Version.Http11), Future.Done)
    assert(!manager.shouldClose())
    val p = Promise[Unit]
    val rep = makeResponse(Version.Http11, "Connection" -> "close")
    rep.setChunked(true)
    manager.observeResponse(rep, p)
    assert(!manager.shouldClose())
    p.setDone()
    assert(manager.shouldClose())
  }

  test("terminate http/1.0 after response") {
    val manager = new ConnectionManager()

    manager.observeRequest(makeRequest(Version.Http10), Future.Done)
    assert(!manager.shouldClose())

    val rep = makeResponse(Version.Http10)
    manager.observeResponse(rep, Future.Unit)
    assert(manager.shouldClose())
    assert(rep.headerMap.get(Fields.Connection) == Some("close"))
  }

  test("not terminate on a response with a status code that must not have a body") {
    val reps = Seq(                        // Status code
      Response(Status.Continue),           // 100
      Response(Status.SwitchingProtocols), // 101
      Response(Status.Processing),         // 102
      Response(Status.NoContent),          // 204
      Response(Status.NotModified)         // 304
    )

    reps.foreach { rep =>
      assert(rep.contentLength == None) // sanity checks
      assert(!rep.isChunked)

      val manager = new ConnectionManager()

      manager.observeRequest(makeRequest(Version.Http11), Future.Done)
      assert(!manager.shouldClose())

      manager.observeResponse(rep, Future.Done)
      assert(!manager.shouldClose())
      assert(rep.headerMap.get(Fields.Connection) == None)
    }
  }

  // these tests are sophisticated, and use things that ConnectionManager
  // isn't aware of.  we should be careful in the way we use it.
  def perform(request: Request, response: Response, shouldMarkDead: Boolean) {
    val closeP = new Promise[Throwable]
    val trans = mock[Transport[Request, Response]]
    when(trans.close(any[Time])).thenReturn(Future.Done)
    when(trans.onClose).thenReturn(closeP)

    val disp = new HttpClientDispatcher(
      new HttpTransport(new IdentityStreamTransport(trans)),
      NullStatsReceiver)

    val wp = new Promise[Unit]
    when(trans.write(any[Request])).thenReturn(wp)

    val rp = new Promise[Response]
    when(trans.read()).thenReturn(rp)

    val f = disp(request)
    assert(f.isDefined == false)

    verify(trans, times(1)).write(any[Request])
    verify(trans, times(1)).read()

    wp.setDone()

    verify(trans, times(1)).read()

    assert(f.isDefined == false)
    rp.setValue(response)

    f.poll match {
      case Some(Return(r)) =>
        assert(r.version == response.version)
        assert(r.status == response.status)

      case _ =>
        fail()
    }

    if (shouldMarkDead)
      verify(trans, times(1)).close(Time.Bottom)
  }

  test("not terminate regular http/1.1 connections") {
    perform(
      makeRequest(Version.Http11),
      makeResponse(Version.Http11, Fields.ContentLength -> "1"),
      false)
  }

  // Note: by way of the codec, this reply is already taken care of.
  test("terminate http/1.1 connections without content length") {
    perform(
      makeRequest(Version.Http11),
      makeResponse(Version.Http11),
      true
    )
  }

  test("terminate http/1.1 connections with Connection: close") {
    perform(
      makeRequest(Version.Http11, "Connection" -> "close"),
      makeResponse(Version.Http11),
      true
    )
  }
}
