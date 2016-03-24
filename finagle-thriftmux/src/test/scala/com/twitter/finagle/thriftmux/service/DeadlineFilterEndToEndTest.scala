package com.twitter.finagle.thriftmux.service

import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.thrift.Protocols
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.util._
import com.twitter.util.TimeConversions._
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class DeadlineFilterEndToEndTest extends FunSuite {

  test("DeadlineFilter per connection") {
    Time.withCurrentTimeFrozen { tc =>
      val statsReceiver = new InMemoryStatsReceiver

      val iface = new TestService.FutureIface {
        def query(x: String) = Future.value(x)
      }
      val svc = new TestService.FinagledService(iface, Protocols.binaryFactory())
      val server = ThriftMux.server
        .withLabel("myservice")
        .withStatsReceiver(statsReceiver)
        .withAdmissionControl.deadlineTolerance(Duration.Top)
        .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), svc)

      val client1 = ThriftMux.client.newIface[TestService.FutureIface](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client1")

      val client2 = ThriftMux.client.newIface[TestService.FutureIface](
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "client2")

      // We set a an expired deadline because DeadlineFilter's Time.now is not frozen; see
      // `withTimeFunction` in c.t.u.Time; we also set a rejection tolerance of Duration.Top to make
      // sure the remaining time DeadlineFilter sees is within the tolerance, so we can reject.
      Contexts.broadcast.let(Deadline, Deadline(Time.now, Time.now - 5.seconds)) {
        // Fill up the reject bucket for client 1
        for (i <- 0 until 5) assert(Await.result(client1.query("ok"), 1.second) == "ok")
        assert(statsReceiver.counters.get(
          List("myservice", "admission_control", "deadline", "rejected")) == None)

        // Fill up the reject bucket for client 2
        for (i <- 0 until 5) assert(Await.result(client2.query("ok"), 1.second) == "ok")
        assert(statsReceiver.counters.get(
          List("myservice", "admission_control", "deadline", "rejected")) == None)

        // With a full reject bucket, Client 1 should be able to reject
        assert(Await.result(client1.query("ok"), 1.second) == "ok")
        assert(statsReceiver.counters.get(
          List("myservice", "admission_control", "deadline", "rejected")) == Some(1))

        // Client 1's reject bucket should be empty; Client 1 should not be able to reject
        assert(Await.result(client1.query("ok"), 1.second) == "ok")
        assert(statsReceiver.counters.get(
          List("myservice", "admission_control", "deadline", "rejected")) == Some(1))

        // With a full reject bucket, Client 2 should be able to reject
        assert(Await.result(client2.query("ok"), 1.second) == "ok")
        assert(statsReceiver.counters.get(
          List("myservice", "admission_control", "deadline", "rejected")) == Some(2))
      }
      server.close()
    }
  }
}