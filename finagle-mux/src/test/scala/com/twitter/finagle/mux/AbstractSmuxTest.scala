package com.twitter.finagle.mux

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.client.EndpointerStackClient
import com.twitter.finagle.mux.transport.{IncompatibleNegotiationException, OpportunisticTls}
import com.twitter.finagle.netty4.channel.ChannelSnooper
import com.twitter.finagle.server.ListeningStackServer
import com.twitter.finagle.ssl.{KeyCredentials, TrustCredentials}
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.io.{Buf, TempFile}
import com.twitter.util.{Await, Closable, Future, Try}
import io.netty.channel.ChannelPipeline
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

// duplicated in ThriftSmuxTest, please update there too
abstract class AbstractSmuxTest extends AnyFunSuite {

  import AbstractSmuxTest._

  type ServerT <: ListeningStackServer[
    mux.Request,
    mux.Response,
    ServerT
  ] with OpportunisticTlsParams[
    ServerT
  ]

  type ClientT <: EndpointerStackClient[
    mux.Request,
    mux.Response,
    ClientT
  ] with OpportunisticTlsParams[
    ClientT
  ]

  def serverImpl(): ServerT

  def clientImpl(): ClientT

  private def serve(serverLevel: Option[OpportunisticTls.Level]): ListeningServer =
    (serverLevel match {
      case None =>
        serverImpl()
      case Some(level) =>
        serverImpl().withTransport
          .tls(mkConfig())
          .withOpportunisticTls(level)
    }).serve("localhost:*", concatService)

  private def newService(
    clientLevel: Option[OpportunisticTls.Level],
    record: ClientT => ClientT,
    stats: StatsReceiver,
    addr: InetSocketAddress
  ): Service[mux.Request, mux.Response] =
    record(
      clientLevel match {
        case None =>
          clientImpl().withStatsReceiver(stats)
        case Some(level) =>
          clientImpl()
            .withStatsReceiver(stats)
            .withTransport.tlsWithoutValidation
            .withOpportunisticTls(level)
      }
    ).newService(Name.bound(Address(addr)), "client")

  private def record(buffer: StringBuffer)(client: ClientT): ClientT = {
    val recordingPrinter: (Stack.Params, ChannelPipeline) => Unit = (params, pipeline) => {
      Mux.Client.tlsEnable(params, pipeline)
      pipeline.addFirst(ChannelSnooper.byteSnooper("whatever") { (string, _) =>
        buffer.append(string)
      })
    }

    client.configured(Mux.param.TurnOnTlsFn(recordingPrinter))
  }

  private def smuxTest(
    testCases: Seq[TlsPair],
    testFn: (Try[Response], String, InMemoryStatsReceiver) => Unit
  ): Unit = {
    for {
      (clientLevel, serverLevel) <- testCases
    } {
      val buffer = new StringBuffer()
      val stats = new InMemoryStatsReceiver
      val server = serve(serverLevel)
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

      val client = newService(clientLevel, record(buffer), stats, addr)
      val results = await(client(request).liftToTry)
      testFn(results, buffer.toString, stats)

      Await.ready(Closable.all(server, client).close(), 5.seconds)
    }
  }

  // tests
  test("smux: can talk to each other with opportunistic tls") {
    smuxTest(
      compatibleEnabledLevels,
      {
        case (results, string, _) =>
          val Buf.Utf8(repString) = results.get.body
          assert(repString == "." * 20)
          // we check that it's non-empty to ensure that it was correctly installed
          assert(!string.isEmpty)
          // check that the payload isn't in cleartext over the wire
          assert(!string.contains("2e" * 10))
      }
    )
  }

  test("smux: can talk to each other when both parties are off") {
    smuxTest(
      compatibleUndesiredDisabledLevels,
      {
        case (results, string, _) =>
          val Buf.Utf8(repString) = results.get.body
          assert(repString == "." * 20)
          assert(string.isEmpty)
      })
  }

  test("smux: can talk to each other when one party is off") {
    smuxTest(
      compatibleDesiredDisabledLevels,
      {
        case (results, string, _) =>
          val Buf.Utf8(repString) = results.get.body
          assert(repString == "." * 20)
          assert(string.isEmpty)
      })
  }

  test("smux: can't create a client with an invalid OppTls config") {
    for (level <- Seq(OpportunisticTls.Required, OpportunisticTls.Desired)) {
      val client = clientImpl()
      intercept[IllegalStateException] {
        client.withOpportunisticTls(level).newService("0.0.0.0:8080")
      }
    }
  }

  test("smux: can't create a server with an invalid OppTls config") {
    for (level <- Seq(OpportunisticTls.Required, OpportunisticTls.Desired)) {
      val server = serverImpl()
      intercept[IllegalStateException] {
        val addr = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
        server
          .withOpportunisticTls(level).serve(addr, Service.const(Future.exception(new Exception())))
      }
    }
  }

  test("smux: can't talk to each other with incompatible opportunistic tls") {
    smuxTest(
      incompatibleLevels,
      {
        case (results, string, stats) =>
          intercept[IncompatibleNegotiationException] {
            results.get
          }
          assert(string.isEmpty)

          assert(stats.counters.get(Seq("client", "failures")) == Some(0))
          assert(stats.counters.get(Seq("client", "service_creation", "failures")) == Some(1))
          assert(
            stats.counters.get(
              Seq(
                "client",
                "service_creation",
                "failures",
                "com.twitter.finagle.mux.transport.IncompatibleNegotiationException"
              )
            ) == Some(1)
          )

      }
    )
  }

  test("smux: server which requires TLS will reject connections if negotiation doesn't happen") {
    val server = serve(Some(OpportunisticTls.Required))
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    val client = new NonNegotiatingClient().newService(Name.bound(Address(addr)), "client")
    val results = await(client(request).liftToTry)

    // TODO: this is not a very helpful exception for diagnosing negotiation failure.
    // A better course of action would be to act as if the service didn't resolve and go into
    // drain mode. Unfortunately, the mux server doesn't implement handshaking at the right
    // level so doing so would be a real pain. This should be much simpler to do in push-mux.
    intercept[ChannelClosedException] {
      results.get
    }

    Await.ready(Closable.all(server, client).close(), 5.seconds)
  }

  test("smux: client which requires TLS will reject connections if negotiation doesn't happen") {
    val server = NonNegotiatingServer().serve("localhost:*", concatService)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    val client = newService(Some(OpportunisticTls.Required), identity, NullStatsReceiver, addr)
    val results = await(client(request).liftToTry)

    intercept[IncompatibleNegotiationException] {
      results.get
    }

    Await.ready(Closable.all(server, client).close(), 5.seconds)
  }
}

private object AbstractSmuxTest {
  type TlsPair = (Option[OpportunisticTls.Level], Option[OpportunisticTls.Level])

  val concatService = new Service[Request, Response] {
    def apply(req: Request) = Future.value(Response(Nil, req.body.concat(req.body)))
  }
  val request = Request(Path.empty, Nil, Buf.Utf8("." * 10))

  def await[A](f: Future[A]): A = Await.result(f, 30.seconds)

  def mkConfig(): SslServerConfiguration = {
    val certFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
    // deleteOnExit is handled by TempFile

    val keyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    SslServerConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(certFile, keyFile),
      trustCredentials = TrustCredentials.Insecure
    )
  }

  // test cases
  val compatibleEnabledLevels: Seq[TlsPair] = {
    val canSpeakTls = Seq(OpportunisticTls.Desired, OpportunisticTls.Required)
    for {
      left <- canSpeakTls
      right <- canSpeakTls
    } yield (Some(left), Some(right))
  }

  val compatibleUndesiredDisabledLevels: Seq[TlsPair] = {
    val noSpeakTls = Seq(Some(OpportunisticTls.Off), None)
    for {
      left <- noSpeakTls
      right <- noSpeakTls
    } yield (left, right)
  }

  val compatibleDesiredDisabledLevels: Seq[TlsPair] = {
    val noSpeakTls = Seq(Some(OpportunisticTls.Off), None)
    val leftNoSpeakTls = noSpeakTls.map((_, Some(OpportunisticTls.Desired)))
    leftNoSpeakTls ++ leftNoSpeakTls.map(_.swap)
  }

  val incompatibleLevels: Seq[TlsPair] = {
    val noSpeakTls = Seq(Some(OpportunisticTls.Off), None)
    val leftNoSpeakTls = noSpeakTls.map((_, Some(OpportunisticTls.Required)))
    leftNoSpeakTls ++ leftNoSpeakTls.map(_.swap)
  }
}
