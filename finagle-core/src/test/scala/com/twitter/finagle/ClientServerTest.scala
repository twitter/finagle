package com.twitter.finagle

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.server.utils.StringServer
import com.twitter.util.{Await, Future, FuturePool}
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.{Executors, ThreadFactory}
import org.scalatest.funsuite.AnyFunSuite

class ClientServerTest extends AnyFunSuite {

  private def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  test("offload execution off of the IO thread") {
    val pool = FuturePool(
      Executors.newFixedThreadPool(
        1,
        new ThreadFactory {
          def newThread(r: Runnable): Thread = new Thread(r, "non-io-thread")
        }
      )
    )

    val s = StringServer.server
      .withExecutionOffloaded(pool)
      .serve(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        Service.mk[String, String](_ => Future.value(Thread.currentThread().getName))
      )

    val c = StringClient.client
      .withExecutionOffloaded(pool)
      .newService(Name.bound(Address(s.boundAddress.asInstanceOf[InetSocketAddress])), "client")

    val (remote, local) =
      await(c("whats-your-thread").map(remote => (remote, Thread.currentThread().getName)))

    assert(remote == local)
    assert(remote == "non-io-thread")

    await(s.close().before(c.close()))
  }
}
