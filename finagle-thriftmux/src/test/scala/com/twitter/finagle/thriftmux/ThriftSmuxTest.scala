package com.twitter.finagle.thriftmux

import com.twitter.conversions.time._
import com.twitter.finagle.mux.transport.{OpportunisticTls, IncompatibleNegotiationException}
import com.twitter.finagle.netty4.channel.ChannelSnooper
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.ssl.{TrustCredentials, KeyCredentials}
import com.twitter.finagle.thriftmux.thriftscala._
import com.twitter.finagle.toggle.flag
import com.twitter.finagle.{Mux, Stack, Name, Address, ListeningServer, ThriftMux}
import com.twitter.io.TempFile
import com.twitter.util.{Future, Await, Closable, Try}
import io.netty.channel.ChannelPipeline
import java.lang.StringBuffer
import java.net.InetSocketAddress
import org.scalatest.FunSuite

// duplicated in SmuxTest, please update there too
class ThriftSmuxTest extends FunSuite {
  import SmuxTest._

  def smuxTest(testCases: Seq[TlsPair], testFn: (Try[String], StringBuffer) => Unit): Unit = {
    for {
      (clientLevel, serverLevel) <- testCases
    } {
      val buffer = new StringBuffer()

      flag.overrides.let(Mux.param.MuxImpl.TlsHeadersToggleId, 1.0) {
        val server = serve(serverLevel)
        val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

        val client = newService(clientLevel, record(buffer), addr)
        val results = await(client.query("." * 10).liftToTry)
        testFn(results, buffer)

        Await.ready(Closable.all(server).close(), 5.seconds)
      }
    }
  }

  // tests
  test("thriftsmux: can talk to each other with opportunistic tls") {
    smuxTest(compatibleEnabledLevels, { case (results, buffer) =>
      assert(results.get == "." * 20)
      // we check that it's non-empty to ensure that it was correctly installed
      assert(!buffer.toString.isEmpty)
      // check that the payload isn't in cleartext over the wire
      assert(!buffer.toString.contains("." * 10))
    })
  }

  test("thriftsmux: can talk to each other when both parties are off") {
    smuxTest(compatibleUndesiredDisabledLevels, { case (results, buffer) =>
      assert(results.get == "." * 20)
      assert(buffer.toString.isEmpty)
    })
  }

  test("thriftsmux: can talk to each other when one party is off") {
    smuxTest(compatibleDesiredDisabledLevels, { case (results, buffer) =>
      assert(results.get == "." * 20)
      assert(buffer.toString.isEmpty)
    })
  }

  test("thriftsmux: can't talk to each other with incompatible opportunistic tls") {
    smuxTest(incompatibleLevels, { case (results, buffer) =>
      intercept[IncompatibleNegotiationException] {
        results.get
      }
      assert(buffer.toString.isEmpty)
    })
  }
}

object SmuxTest {
  type TlsPair = (Option[OpportunisticTls.Level], Option[OpportunisticTls.Level])

  val concatIface = new TestService.FutureIface {
    def query(x: String): Future[String] = Future.value(x.concat(x))
  }

  def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  def serve(
    serverLevel: Option[OpportunisticTls.Level]
  ): ListeningServer = (serverLevel match {
    case None =>
      ThriftMux.server
    case Some(level) =>
      ThriftMux.server
        .withTransport.tls(mkConfig())
        .withOpportunisticTls(level)
  }).serveIface("localhost:*", concatIface)

  def newService(
    clientLevel: Option[OpportunisticTls.Level],
    record: ThriftMux.Client => ThriftMux.Client,
    addr: InetSocketAddress
  ) = record(
    clientLevel match {
      case None =>
        ThriftMux.client
      case Some(level) =>
        ThriftMux.client
          .withTransport.tlsWithoutValidation
          .withOpportunisticTls(level)
    }
  ).newIface[TestService.FutureIface](
    Name.bound(Address(addr)),
    "client"
  )

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
      pipeline.addFirst(ChannelSnooper.byteSnooper("whatever") { (string, _) =>
        buffer.append(string)
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
