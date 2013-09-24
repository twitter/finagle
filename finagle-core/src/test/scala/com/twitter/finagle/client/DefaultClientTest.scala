package com.twitter.finagle.client

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{Client, Group, SourcedException, Service}
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.util.{Await, Future}
import com.twitter.finagle.stats.StatsReceiver
import java.net.{SocketAddress, InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DefaultClientTest extends FunSpec {
  describe("DefaultClient") {
    it("should successfully add sourcedexception") {
      val name = "name"
      val qIn = new AsyncQueue[Int]()
      val qOut = new AsyncQueue[Int]()
      val transporter: (SocketAddress, StatsReceiver) => Future[Transport[Int, Int]] = { case (_, _) =>
        Future.value(new QueueTransport(qIn, qOut))
      }
      val dispatcher: Transport[Int, Int] => Service[Int, Int] = { _ =>
        Service.mk { _ =>
          throw new SourcedException{}
        }
      }
      val endPointer = Bridge[Int, Int, Int, Int](transporter, dispatcher)
      val client: Client[Int, Int] = DefaultClient[Int, Int](name, endPointer)
      val socket = new SocketAddress(){}
      val group = Group(socket)
      val service: Service[Int, Int] = Await.result(client.newClient(group)())
      val f = service(3)
      qOut.offer(3)
      val e = intercept[SourcedException] {
        Await.result(f)
      }
      assert(e.serviceName === name)
    }
  }
}
