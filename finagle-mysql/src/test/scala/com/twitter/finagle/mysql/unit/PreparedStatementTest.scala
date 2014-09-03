package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Await, Closable, Future, Time}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class PrepareCacheTest extends FunSuite with MockitoSugar {
  test("cache prepare requests") {
    val dispatcher = mock[Service[Request, Result]]
    val stmtId = 2
    when(dispatcher(any[Request])).thenReturn(Future.value(
      PrepareOK(stmtId, 1, 1, 0)))

    val svc = new PrepareCache(dispatcher, 11)
    val r0 = PrepareRequest("SELECT 0")
    svc(r0)
    svc(r0)
    verify(dispatcher, times(1)).apply(r0)

    for (i <- 1 to 10) svc(PrepareRequest("SELECT %d".format(i)))
    svc(PrepareRequest("SELECT 5"))
    verify(dispatcher, times(1)).apply(PrepareRequest("SELECT 5"))

    // dispatch current eldest.
    // we should maintain access order.
    svc(r0)
    verify(dispatcher, times(1)).apply(r0)

    // fill cache and evict eldest.
    svc(PrepareRequest("SELECT 11"))
    verify(dispatcher, times(1)).apply(CloseRequest(stmtId))

    // evicted element is not in cache.
    svc(PrepareRequest("SELECT 1"))
    verify(dispatcher, times(2)).apply(PrepareRequest("SELECT 1"))
  }
}
