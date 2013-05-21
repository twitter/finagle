package com.twitter.finagle.benchmark

import java.net.SocketAddress
import com.twitter.finagle.{Group, ServiceFactory, Service}
import com.twitter.finagle.client.DefaultClient
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Await, Future}
import com.google.caliper.{SimpleBenchmark, Param}

object EchoBridge {
  def apply[Req]: ((SocketAddress, StatsReceiver) => ServiceFactory[Req, Req]) =
    (sa, sr) => ServiceFactory(() => Future.value(new Service[Req, Req] {
      def apply(req: Req) = Future.value(req)
    }))
}

object TestClient extends DefaultClient[Int, Int](
  name = "test",
  endpointer = EchoBridge[Int]
)

class DefaultClientBenchmark extends SimpleBenchmark {
  @Param(Array("1", "10")) val nconns: Int = 1

  val addrs = (0 until nconns) map { _ => new SocketAddress {} }
  val clientWithDefaultStack = TestClient.newService(Group(addrs: _*))

  def timeClientWithDefaultStack(n: Int) {
    var i = 0
    while (i < n) {
      Await.ready(clientWithDefaultStack(i))
      i += 1
    }
  }
}
