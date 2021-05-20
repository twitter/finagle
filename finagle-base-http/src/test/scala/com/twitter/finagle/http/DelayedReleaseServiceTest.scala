package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.util.{Await, Duration, Future}
import org.mockito.Matchers.any
import org.mockito.Mockito.{never, stub, verify}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class DelayedReleaseServiceTest extends AnyFunSuite with MockitoSugar {

  def await[A](f: Future[A]): A = Await.result(f, Duration.fromSeconds(30))

  test("close closes underlying") {
    val service = mock[Service[Request, Response]]
    val proxy = new DelayedReleaseService(service)
    stub(service.close()).toReturn(Future.Done)

    proxy.close()
    verify(service).close()
  }

  test("close waits for response completion") {
    val response = Response()
    response.setChunked(true)
    response.writer.close()

    val service = mock[Service[Request, Response]]
    stub(service.close()).toReturn(Future.Done)
    stub(service.apply(any[Request])).toReturn(Future.value(response))

    val proxy = new DelayedReleaseService(service)

    val result = proxy(Request()).flatMap { _ =>
      proxy.close()
      verify(service, never).close()
      response.reader.read()
    }

    assert(await(result).isEmpty)
    verify(service).close()
  }

  test("close waits for request completion") {
    val request = Request()
    request.setChunked(true)
    request.writer.close()

    val service = mock[Service[Request, Response]]
    stub(service.close()).toReturn(Future.Done)
    stub(service.apply(any[Request])).toReturn(Future.value(Response()))

    val proxy = new DelayedReleaseService(service)

    val result = proxy(request).flatMap { _ =>
      proxy.close()
      verify(service, never).close()
      request.reader.read()
    }

    assert(await(result).isEmpty)
    verify(service).close()
  }

  test("close waits for request & response completion") {
    val request = Request()
    request.setChunked(true)
    request.writer.close()

    val response = Response()
    response.setChunked(true)
    response.writer.close()

    val service = mock[Service[Request, Response]]
    stub(service.close()).toReturn(Future.Done)
    stub(service.apply(any[Request])).toReturn(Future.value(response))

    val proxy = new DelayedReleaseService(service)

    val result = proxy(request).flatMap { _ =>
      proxy.close()
      verify(service, never).close()

      request.reader.read().flatMap { _ =>
        verify(service, never).close()

        response.reader.read()
      }
    }

    assert(await(result).isEmpty)
    verify(service).close()
  }

  test("inner service failure") {
    val service = mock[Service[Request, Response]]
    val proxy = new DelayedReleaseService(service)
    stub(service.close()).toReturn(Future.Done)
    stub(service.apply(any[Request])).toReturn(Future.exception(new Exception))

    val request = Request()

    proxy(request)
    proxy.close()
    verify(service).close()
  }
}
