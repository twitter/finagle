package com.twitter.finagle.builder

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.service.RetryPolicy
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.client.SslClientEngineFactory
import com.twitter.finagle.ssl.client.SslClientSessionVerifier
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.SSLSession
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.when
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ClientBuilderTest
    extends AnyFunSuite
    with Eventually
    with IntegrationPatience
    with MockitoSugar {

  private class MyException extends Exception

  private val retryMyExceptionOnce = RetryPolicy.tries[Try[Nothing]](
    2, // 2 tries == 1 attempt + 1 retry
    { case Throw(_: MyException) => true }
  )

  test("ClientBuilder should collect stats on 'tries' for retryPolicy") {
    val service = mock[Service[String, String]]
    when(service("123")) thenReturn Future.exception(new MyException())
    when(service.close(any[Time])) thenReturn Future.Done

    val inMemory = new InMemoryStatsReceiver
    val builder = ClientBuilder()
      .stack(StringClient.client.withEndpoint(service))
      .failFast(false)
      .name("test")
      .hostConnectionLimit(1)
      .daemon(true) // don't create an exit guard
      .hosts(Seq(new InetSocketAddress(0)))
      .retryPolicy(retryMyExceptionOnce)
      .reportTo(inMemory)

    val client = builder.build()

    Await.ready(client("123"), 10.seconds)

    assert(inMemory.counters(Seq("test", "tries", "requests")) == 1)

    // 1 request + 1 retry
    assert(inMemory.counters(Seq("test", "requests")) == 2)
  }

  test("ClientBuilder should collect stats on 'tries' with no retryPolicy") {
    val service = mock[Service[String, String]]
    when(service("123")) thenReturn Future.exception(WriteException(new Exception()))
    when(service.close(any[Time])) thenReturn Future.Done
    when(service.status) thenReturn Status.Open

    val inMemory = new InMemoryStatsReceiver
    val numFailures = 21 // There will be 20 requeues by default
    val builder = ClientBuilder()
      .stack(StringClient.client.withEndpoint(service))
      .failFast(false)
      .name("test")
      .hostConnectionLimit(1)
      .daemon(true) // don't create an exit guard
      .hosts(Seq(new InetSocketAddress(0)))
      .failureAccrualParams(25 -> Duration.fromSeconds(10))
      .reportTo(inMemory)

    val client = builder.build()

    Await.ready(client("123"), 10.seconds)

    assert(inMemory.counters(Seq("test", "tries", "requests")) == 1)
    // failure accrual marks the only node in the balancer as Busy which in turn caps requeues
    // this relies on a retry budget that allows for `numFailures` requeues
    assert(inMemory.counters(Seq("test", "requests")) == numFailures)
  }

  private class SpecificException extends RuntimeException

  test("Retries have locals propagated") {
    val specificExceptionRetry: PartialFunction[Try[Nothing], Boolean] = {
      case Throw(e: SpecificException) => true
    }

    val aLocal = new Local[Int]
    val first = new AtomicBoolean(false)
    val localOnRetry = new AtomicInteger(0)

    // 1st call fails and triggers a retry which
    // captures the value of the local
    val service = Service.mk[String, String] { str: String =>
      if (first.compareAndSet(false, true)) {
        Future.exception(new SpecificException())
      } else {
        localOnRetry.set(aLocal().getOrElse(-1))
        Future(str)
      }
    }

    val builder = ClientBuilder()
      .stack(StringClient.client.withEndpoint(service))
      .failFast(false)
      .name("test")
      .hostConnectionLimit(1)
      .daemon(true) // don't create an exit guard
      .hosts(Seq(new InetSocketAddress(0)))
      .retryPolicy(RetryPolicy.tries(2, specificExceptionRetry))
      .reportTo(NullStatsReceiver)
    val client = builder.build()

    aLocal.let(999) {
      val rep = client("hi")
      assert("hi" == Await.result(rep, Duration.fromSeconds(5)))
    }
    assert(999 == localOnRetry.get)
  }

  test("configured") {
    val cb = ClientBuilder()
    assert(!cb.params.contains[ProtocolLibrary])
    val configured = cb.configured(ProtocolLibrary("derp"))
    assert(configured.params.contains[ProtocolLibrary])
    assert("derp" == configured.params[ProtocolLibrary].name)
  }

  test("ClientBuilder should close properly") {
    val svc = ClientBuilder().stack(StringClient.client).hostConnectionCoresize(1).hosts("").build()
    val f = svc.close()
    eventually { f.isDefined }
  }

  private val config = SslClientConfiguration()
  private val engine = mock[Engine]
  private val engineFactory = new SslClientEngineFactory {
    def apply(address: Address, config: SslClientConfiguration): Engine = engine
  }
  private val sessionVerifier = new SslClientSessionVerifier {
    def apply(address: Address, config: SslClientConfiguration, session: SSLSession): Boolean = true
  }

  test("ClientBuilder sets SSL/TLS configuration") {
    val client = ClientBuilder().tls(config)
    assert(client.params[Transport.ClientSsl].sslClientConfiguration == Some(config))
  }

  test("ClientBuilder sets SSL/TLS configuration, engine factory") {
    val client = ClientBuilder().tls(config, engineFactory)
    assert(client.params[Transport.ClientSsl].sslClientConfiguration == Some(config))
    assert(client.params[SslClientEngineFactory.Param].factory == engineFactory)
  }

  test("ClientBuilder sets SSL/TLS configuration, verifier") {
    val client = ClientBuilder().tls(config, sessionVerifier)
    assert(client.params[Transport.ClientSsl].sslClientConfiguration == Some(config))
    assert(client.params[SslClientSessionVerifier.Param].verifier == sessionVerifier)
  }

  test("ClientBuilder sets SSL/TLS configuration, engine factory, verifier") {
    val client = ClientBuilder().tls(config, engineFactory, sessionVerifier)
    assert(client.params[Transport.ClientSsl].sslClientConfiguration == Some(config))
    assert(client.params[SslClientEngineFactory.Param].factory == engineFactory)
    assert(client.params[SslClientSessionVerifier.Param].verifier == sessionVerifier)
  }
}
