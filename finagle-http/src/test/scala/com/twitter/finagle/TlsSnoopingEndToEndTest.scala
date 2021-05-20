package com.twitter.finagle

import com.twitter.conversions.DurationOps._
import com.twitter.finagle
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.ssl.HttpSslTestComponents
import com.twitter.finagle.param.OppTls
import com.twitter.finagle.ssl.{ClientAuth, OpportunisticTls, SnoopingLevelInterpreter, TlsSnooping}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Awaitable, Future}
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class TlsSnoopingEndToEndTest extends AnyFunSuite {

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  def serverImpl(
    useH2: Boolean,
    clientAuth: ClientAuth,
    level: OpportunisticTls.Level,
    snooper: TlsSnooping.Snooper
  ): finagle.Http.Server = {
    val base =
      finagle.Http.server.configured(SnoopingLevelInterpreter.EnabledForNonNegotiatingProtocols)
    (if (useH2) base.withHttp2 else base.withNoHttp2)
      .configured(Transport.ServerSsl(
        Some(HttpSslTestComponents.unauthenticatedServerConfig.copy(clientAuth = clientAuth))))
      .configured(OppTls(Some(level)))
      .configured(TlsSnooping.Param(snooper))
  }

  def clientImpl(useH2: Boolean): finagle.Http.Client =
    if (useH2) finagle.Http.client.withHttp2
    else finagle.Http.client.withNoHttp2

  private def doADispatch(
    useH2: Boolean,
    clientAuth: ClientAuth,
    level: OpportunisticTls.Level,
    snooper: TlsSnooping.Snooper
  ): Response = {
    val server = serverImpl(useH2, clientAuth, level, snooper)
      .serve("localhost:*", Service.mk { _: Request => Future.value(Response()) })
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    val client = clientImpl(useH2)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    try {
      await(client(Request()))
      // Do a second request since in the case of H2C we need to upgrade the
      // connection to H2 via the initial request.
      await(client(Request()))
    } finally {
      await(client.close())
      await(server.close())
    }
  }

  private def testDispatch(
    useH2: Boolean,
    clientAuth: ClientAuth,
    level: OpportunisticTls.Level,
    snooper: TlsSnooping.Snooper
  ): Unit = {
    if (shouldFail(clientAuth, level)) {
      assertThrows[ChannelClosedException] {
        doADispatch(useH2, clientAuth, level, snooper)
      }
    } else {
      doADispatch(useH2, clientAuth, level, snooper)
    }
  }

  def combos: Seq[(Boolean, ClientAuth, OpportunisticTls.Level, TlsSnooping.Snooper)] = {
    val authOptions = Seq(
      ClientAuth.Unspecified,
      ClientAuth.Off,
      ClientAuth.Wanted,
      ClientAuth.Needed
    )
    val opportunisticLevels = Seq(
      OpportunisticTls.Off,
      OpportunisticTls.Desired,
      OpportunisticTls.Required
    )

    val snoopers = Seq(TlsSnooping.Tls1XDetection)

    for {
      auth <- authOptions
      level <- opportunisticLevels
      snooper <- snoopers
      useH2 <- Seq(true, false)
    } yield (useH2, auth, level, snooper)
  }

  private[this] def shouldFail(clientAuth: ClientAuth, level: OpportunisticTls.Level): Boolean = {
    clientAuth == ClientAuth.Needed || level != OpportunisticTls.Desired
  }

  // Generate the tests
  combos.foreach {
    case (useH2, auth, level, snooper) =>
      test(s"H2:$useH2, Client auth: $auth, Opportunistic level: $level") {
        testDispatch(useH2, auth, level, snooper)
      }
  }
}
