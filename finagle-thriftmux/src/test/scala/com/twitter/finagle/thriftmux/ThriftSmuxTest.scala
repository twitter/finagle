package com.twitter.finagle.thriftmux

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.mux.transport.{IncompatibleNegotiationException, OpportunisticTls}
import com.twitter.finagle.netty4.channel.ChannelSnooper
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.ssl.{KeyCredentials, TrustCredentials}
import com.twitter.finagle.thriftmux.thriftscala._
import com.twitter.finagle._
import com.twitter.finagle.stats.{InMemoryStatsReceiver, StatsReceiver}
import com.twitter.io.TempFile
import com.twitter.util.{Await, Future, Try}
import io.netty.channel.ChannelPipeline
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

// duplicated in SmuxTest, please update there too
class ThriftSmuxTest extends AnyFunSuite {
  import ThriftSmuxTest._

  protected def serverImpl(): ThriftMux.Server = ThriftMux.server

  protected def clientImpl(): ThriftMux.Client =
    ThriftMux.client.copy(muxer = ThriftMux.Client.standardMuxer)

  private def serve(serverLevel: Option[OpportunisticTls.Level]): ListeningServer =
    (serverLevel match {
      case None =>
        serverImpl()
      case Some(level) =>
        serverImpl().withTransport
          .tls(mkConfig())
          .withOpportunisticTls(level)
    }).serveIface("localhost:*", concatIface)

  private def newService(
    clientLevel: Option[OpportunisticTls.Level],
    record: ThriftMux.Client => ThriftMux.Client,
    stats: StatsReceiver,
    addr: InetSocketAddress
  ): TestService.MethodPerEndpoint =
    record(
      clientLevel match {
        case None =>
          clientImpl()
            .withStatsReceiver(stats)
        case Some(level) =>
          clientImpl()
            .withStatsReceiver(stats)
            .withTransport.tlsWithoutValidation
            .withOpportunisticTls(level)
      }
    ).build[TestService.MethodPerEndpoint](
      Name.bound(Address(addr)),
      "client"
    )

  def smuxTest(
    testCases: Seq[TlsPair],
    testFn: (Try[String], String, InMemoryStatsReceiver) => Unit
  ): Unit = {
    for {
      (clientLevel, serverLevel) <- testCases
    } {
      withClue(s"clientOppTlsLevel=$clientLevel serverOppTlsLevel=$serverLevel") {
        val buffer = new StringBuffer()
        val stats = new InMemoryStatsReceiver
        val server = serve(serverLevel)
        val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

        val client = newService(clientLevel, record(buffer), stats, addr)
        val results = await(client.query("." * 10).liftToTry)
        testFn(results, buffer.toString, stats)

        Await.ready(server.close(), 10.seconds)
      }
    }
  }

  // tests
  test("thriftsmux: can talk to each other with opportunistic tls") {
    smuxTest(
      compatibleEnabledLevels,
      {
        case (results, string, _) =>
          assert(results.get == "." * 20)
          // we check that it's non-empty to ensure that it was correctly installed
          assert(!string.isEmpty)
          // check that the payload isn't in cleartext over the wire
          assert(!string.toString.contains("2e" * 10))
      }
    )
  }

  test("thriftsmux: can talk to each other when both parties are off") {
    smuxTest(
      compatibleUndesiredDisabledLevels,
      {
        case (results, string, _) =>
          assert(results.get == "." * 20)
          assert(string.isEmpty)
      })
  }

  test("thriftsmux: can talk to each other when one party is off") {
    smuxTest(
      compatibleDesiredDisabledLevels,
      {
        case (results, string, _) =>
          assert(results.get == "." * 20)
          assert(string.isEmpty)
      })
  }

  test("thriftsmux: can't create a client with an invalid OppTls config") {
    for (level <- Seq(OpportunisticTls.Required, OpportunisticTls.Desired)) {
      withClue(s"level=$level") {
        val client = clientImpl()
        intercept[IllegalStateException] {
          val addr = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
          client
            .withOpportunisticTls(level)
            .build[TestService.MethodPerEndpoint](
              Name.bound(Address(addr)),
              "client"
            )
        }
      }
    }
  }

  test("thriftsmux: can't create a server with an invalid OppTls config") {
    for (level <- Seq(OpportunisticTls.Required, OpportunisticTls.Desired)) {
      withClue(s"level=$level") {
        val server = serverImpl()
        intercept[IllegalStateException] {
          val addr = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
          server.withOpportunisticTls(level).serveIface(addr, concatIface)
        }
      }
    }
  }

  test("thriftsmux: can't talk to each other with incompatible opportunistic tls") {
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
}

object ThriftSmuxTest {
  type TlsPair = (Option[OpportunisticTls.Level], Option[OpportunisticTls.Level])

  val concatIface = new TestService.MethodPerEndpoint {
    def query(x: String): Future[String] = Future.value(x.concat(x))
    def question(y: String): Future[String] = ???
    def inquiry(y: String): Future[String] = ???
  }

  def await[A](f: Future[A]): A = Await.result(f, 10.seconds)

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

  def record(buffer: StringBuffer)(client: ThriftMux.Client): ThriftMux.Client = {
    val recordingPrinter: (Stack.Params, ChannelPipeline) => Unit = (params, pipeline) => {
      Mux.Client.tlsEnable(params, pipeline)
      pipeline.addFirst(ChannelSnooper.byteSnooper("snoop") { (string, _) =>
        buffer.append(string + "\n")
      })
    }

    client.configured(Mux.param.TurnOnTlsFn(recordingPrinter))
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
