package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.io.Reader
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, stub, verify}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DelayedReleaseServiceTest extends FunSuite with MockitoSugar {

  test("close closes underlying") {
    val service = mock[Service[Request, Response]]
    val proxy = new DelayedReleaseService(service)
    stub(service.close()).toReturn(Future.Done)

    proxy.close()
    verify(service).close()
  }

  test("close waits for response completion") {
    val request = Request()
    request.response.setChunked(true)

    val service = mock[Service[Request,Response]]
    stub(service.close()).toReturn(Future.Done)
    stub(service.apply(any[Request])).toReturn(Future.value(request.response))

    val proxy = new DelayedReleaseService(service)

    val f = proxy(request) flatMap { response =>
      proxy.close()
      verify(service, never).close()
      Reader.readAll(response.reader)
    }
    request.response.close() // EOF
    verify(service).close()
  }

  test("inner service failure") {
    val service = mock[Service[Request, Response]]
    val proxy = new DelayedReleaseService(service)
    stub(service.close()).toReturn(Future.Done)
    stub(service.apply(any[Request])).toReturn(Future.exception(new Exception))

    val request = Request()
    request.response.setChunked(true)
    proxy(request)
    proxy.close()
    verify(service).close()
  }
}
