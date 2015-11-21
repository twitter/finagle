package com.twitter.finagle.builder

import com.twitter.finagle._
import com.twitter.finagle.integration.IntegrationBase
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.service.{RetryPolicy, FailureAccrualFactory}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import com.twitter.util.registry.{GlobalRegistry, SimpleRegistry}
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import org.junit.runner.RunWith
import org.mockito.Mockito.{verify, when}
import org.mockito.Matchers
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

@RunWith(classOf[JUnitRunner])
class ClientBuilderTest extends FunSuite
  with Eventually
  with IntegrationPatience
  with MockitoSugar
  with IntegrationBase {

  trait ClientBuilderHelper {
    val preparedFactory = mock[ServiceFactory[String, String]]
    val preparedServicePromise = new Promise[Service[String, String]]
    when(preparedFactory.status) thenReturn Status.Open
    when(preparedFactory()) thenReturn preparedServicePromise
    when(preparedFactory.close(any[Time])) thenReturn Future.Done
    when(preparedFactory.map(Matchers.any())) thenReturn
      preparedFactory.asInstanceOf[ServiceFactory[Any, Nothing]]

    val m = new MockChannel
    when(m.codec.prepareConnFactory(any[ServiceFactory[String, String]])) thenReturn preparedFactory
  }

  test("ClientBuilder should invoke prepareConnFactory on connection") {
    new ClientBuilderHelper {
      val client = m.build()
      val requestFuture = client("123")

      verify(m.codec).prepareConnFactory(any[ServiceFactory[String, String]])
      verify(preparedFactory)()

      assert(!requestFuture.isDefined)
      val service = mock[Service[String, String]]
      when(service("123")) thenReturn Future.value("321")
      when(service.close(any[Time])) thenReturn Future.Done
      preparedServicePromise() = Return(service)

      verify(service)("123")
      assert(requestFuture.poll == Some(Return("321")))
    }
  }


  def verifyProtocolRegistry(name: String, expected: String)(build: => Service[String, String]) = {
    test(name + " registers protocol library") {
      val simple = new SimpleRegistry()
      GlobalRegistry.withRegistry(simple) {
        build

        val entries = GlobalRegistry.get.toSet
        val unspecified = entries.count(_.key.startsWith(Seq("client", "not-specified")))
        assert(unspecified == 0, "saw registry keys with 'not-specified' protocol")
        val specified = entries.count(_.key.startsWith(Seq("client", expected)))
        assert(specified > 0, "did not see expected protocol registry keys")
      }
    }
  }

  verifyProtocolRegistry("#codec(Codec)", expected = "fancy") {
    val ctx = new ClientBuilderHelper {}
    when(ctx.m.codec.protocolLibraryName).thenReturn("fancy")

    ClientBuilder()
      .name("test")
      .hostConnectionLimit(1)
      .codec(ctx.m.codec)
      .hosts("")
      .build()
  }

  verifyProtocolRegistry("#codec(CodecFactory)", expected = "fancy") {
    val ctx = new ClientBuilderHelper {}
    val cf = new CodecFactory[String, String] {
      def client: Client = (_: ClientCodecConfig) => ctx.m.codec
      def server: Server = ???
      override def protocolLibraryName = "fancy"
    }

    ClientBuilder()
      .name("test")
      .hostConnectionLimit(1)
      .codec(cf)
      .hosts("")
      .build()
  }

  verifyProtocolRegistry("#codec(CodecFactory#Client)", expected = "fancy") {
    val ctx = new ClientBuilderHelper {}
    when(ctx.m.codec.protocolLibraryName).thenReturn("fancy")

    val cfClient: CodecFactory[String, String]#Client =
      { (_: ClientCodecConfig) => ctx.m.codec }

    ClientBuilder()
      .name("test")
      .hostConnectionLimit(1)
      .codec(cfClient)
      .hosts("")
      .build()
  }

  verifyProtocolRegistry("configured protocol", expected = "extra fancy") {
    val ctx = new ClientBuilderHelper {}
    when(ctx.m.codec.protocolLibraryName).thenReturn("fancy")

    val cfClient: CodecFactory[String, String]#Client =
      { (_: ClientCodecConfig) => ctx.m.codec }

    val stk = ClientBuilder.stackClientOfCodec(cfClient)
    ClientBuilder()
      .name("test")
      .hostConnectionLimit(1)
      .hosts("")
      .stack(stk.configured(ProtocolLibrary("extra fancy")))
      .build()
  }


  test("ClientBuilder should close properly") {
    new ClientBuilderHelper {
      val svc = ClientBuilder().hostConnectionLimit(1).codec(m.codec).hosts("").build()
      val f = svc.close()
      eventually {
        f.isDefined
      }
    }
  }

  private class MyException extends Exception

  private val retryMyExceptionOnce = RetryPolicy.tries[Try[Nothing]](
    2, // 2 tries == 1 attempt + 1 retry
    { case Throw(_: MyException) => true })

  test("ClientBuilder should collect stats on 'tries' for retrypolicy") {
    new ClientBuilderHelper {
      val inMemory = new InMemoryStatsReceiver
      val builder = ClientBuilder()
        .name("test")
        .hostConnectionLimit(1)
        .codec(m.codec)
        .daemon(true) // don't create an exit guard
        .hosts(Seq(m.clientAddress))
        .retryPolicy(retryMyExceptionOnce)
        .reportTo(inMemory)
      val client = builder.build()

      val service = mock[Service[String, String]]
      when(service("123")) thenReturn Future.exception(new MyException())
      when(service.close(any[Time])) thenReturn Future.Done
      preparedServicePromise() = Return(service)

      val f = client("123")

      eventually { assert(f.isDefined) }
      assert(inMemory.counters(Seq("test", "tries", "requests")) == 1)
      assert(
        // 1 request and 1 retry
        inMemory.counters(Seq("test", "requests")) == 2
      )
    }
  }

  test("ClientBuilder should collect stats on 'tries' with no retrypolicy") {
    new ClientBuilderHelper {
      val inMemory = new InMemoryStatsReceiver
      val builder = ClientBuilder()
        .name("test")
        .hostConnectionLimit(1)
        .codec(m.codec)
        .daemon(true) // don't create an exit guard
        .hosts(Seq(m.clientAddress))
        .reportTo(inMemory)

      val client = builder.build()
      val numFailures = 5

      val service = mock[Service[String, String]]
      when(service("123")) thenReturn Future.exception(WriteException(new Exception()))
      when(service.close(any[Time])) thenReturn Future.Done
      preparedServicePromise() = Return(service)

      val f = client("123")

      assert(f.isDefined)
      assert(inMemory.counters(Seq("test", "tries", "requests")) == 1)

      // failure accrual marks the only node in the balancer as Busy which in turn caps requeues
      // this relies on a retry budget that allows for `numFailures` requeues
      assert(inMemory.counters(Seq("test", "requests")) == numFailures)
    }
  }

  test("ClientBuilder with stack should collect stats on 'tries' for retrypolicy") {
    new ClientBuilderHelper {
      val inMemory = new InMemoryStatsReceiver
      val builder = ClientBuilder()
        .name("test")
        .hostConnectionLimit(1)
        .stack(m.client)
        .daemon(true) // don't create an exit guard
        .hosts(Seq(m.clientAddress))
        .retryPolicy(retryMyExceptionOnce)
        .reportTo(inMemory)
      val client = builder.build()

      val service = mock[Service[String, String]]
      when(service("123")) thenReturn Future.exception(new MyException())
      when(service.close(any[Time])) thenReturn Future.Done
      preparedServicePromise() = Return(service)

      val f = client("123")

      eventually { assert(f.isDefined) }
      assert(inMemory.counters(Seq("test", "tries", "requests")) == 1)

      // 1 request + 1 retry
      assert(inMemory.counters(Seq("test", "requests")) == 2)
    }
  }

  test("ClientBuilder with stack should collect stats on 'tries' with no retrypolicy") {
    new ClientBuilderHelper {
      val inMemory = new InMemoryStatsReceiver
      val numFailures = 21  // There will be 20 requeues by default
      val builder = ClientBuilder()
        .name("test")
        .hostConnectionLimit(1)
        .stack(m.client)
        .daemon(true) // don't create an exit guard
        .hosts(Seq(m.clientAddress))
        .failureAccrualParams(25 -> Duration.fromSeconds(10))
        .reportTo(inMemory)

      val client = builder.build()

      val service = mock[Service[String, String]]
      when(service("123")) thenReturn Future.exception(WriteException(new Exception()))
      when(service.close(any[Time])) thenReturn Future.Done
      preparedServicePromise() = Return(service)

      val f = client("123")

      assert(f.isDefined)
      assert(inMemory.counters(Seq("test", "tries", "requests")) == 1)
      // failure accrual marks the only node in the balancer as Busy which in turn caps requeues
      // this relies on a retry budget that allows for `numFailures` requeues
      assert(inMemory.counters(Seq("test", "requests")) == numFailures)
    }
  }

  private class SpecificException extends RuntimeException

  test("Retries have locals propagated") {

    new ClientBuilderHelper {
      val specificExceptionRetry: PartialFunction[Try[Nothing], Boolean] = {
        case Throw(e: SpecificException) => true
      }
      val builder = ClientBuilder()
        .name("test")
        .hostConnectionLimit(1)
        .stack(m.client)
        .daemon(true) // don't create an exit guard
        .hosts(Seq(m.clientAddress))
        .retryPolicy(RetryPolicy.tries(2, specificExceptionRetry))
        .reportTo(NullStatsReceiver)
      val client = builder.build()

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
      preparedServicePromise() = Return(service)

      aLocal.let(999) {
        val rep = client("hi")
        assert("hi" == Await.result(rep, Duration.fromSeconds(5)))
      }
      assert(999 == localOnRetry.get)
    }
  }
}
