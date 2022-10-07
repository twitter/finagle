package com.twitter.finagle.factory

import com.twitter.finagle.ClientConnection
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.Await
import com.twitter.util.Future
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class StatsFactoryWrapperTest extends AnyFunSuite with MockitoSugar {
  val underlying = mock[ServiceFactory[Int, Int]]
  val rex = new RuntimeException
  val t = new Throwable(rex)

  test("report exceptions on Service creation failure") {
    val receiver = new InMemoryStatsReceiver
    val statsFac = new StatsFactoryWrapper(underlying, receiver)

    when(underlying(any[ClientConnection])) thenReturn Future.exception(t)

    intercept[Throwable] {
      Await.result(statsFac(ClientConnection.nil))
    }

    val expected = Map(List("failures", t.getClass.getName, rex.getClass.getName) -> 1)
    assert(receiver.counters == expected)
    verify(underlying)(ClientConnection.nil)
  }
}
