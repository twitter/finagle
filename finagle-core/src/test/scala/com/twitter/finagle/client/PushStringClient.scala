package com.twitter.finagle.client

import com.twitter.finagle.{Service, ServiceFactory, Stack}
import com.twitter.finagle.client.StringClient.{NoDelimStringPipeline, StringClientPipeline}
import com.twitter.finagle.exp.pushsession.{PipeliningClientPushSession, PushChannelHandle, PushStackClient, PushTransporter}
import com.twitter.finagle.netty4.exp.pushsession.Netty4PushTransporter
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Duration, Future}
import java.net.InetSocketAddress

object PushStringClient {

  val protocolLibrary = StringClient.protocolLibrary

  case class Client(
    stack: Stack[ServiceFactory[String, String]] = StackClient.newStack,
    params: Stack.Params = Stack.Params.empty + ProtocolLibrary(protocolLibrary),
    appendDelimeter: Boolean = true
  ) extends PushStackClient[String, String, Client] {
    protected def copy1(
      stack: Stack[ServiceFactory[String, String]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = String
    protected type Out = String
    protected type SessionT = PipeliningClientPushSession[String, String]

    protected def newPushTransporter(ia: InetSocketAddress): PushTransporter[String, String] = {
      val init = if (appendDelimeter) StringClientPipeline else NoDelimStringPipeline
      Netty4PushTransporter.raw[String, String](init, ia, params)
    }

    protected def newSession(handle: PushChannelHandle[String, String]): Future[SessionT] =
      Future.value(
        new PipeliningClientPushSession[String, String](
          handle,
          NullStatsReceiver,
          Duration.Top,
          DefaultTimer
        )
      )

    protected def toService(session: SessionT): Future[Service[String, String]] =
      Future.value(session.toService)
  }

  val client: Client = Client()
}
