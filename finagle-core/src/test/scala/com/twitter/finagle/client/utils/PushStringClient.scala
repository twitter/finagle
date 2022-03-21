package com.twitter.finagle.client.utils

import com.twitter.finagle.client.StackClient
import com.twitter.finagle.client.utils.StringClient.NoDelimStringPipeline
import com.twitter.finagle.client.utils.StringClient.StringClientPipeline
import com.twitter.finagle.pushsession.PipeliningClientPushSession
import com.twitter.finagle.pushsession.PushChannelHandle
import com.twitter.finagle.pushsession.PushStackClient
import com.twitter.finagle.pushsession.PushTransporter
import com.twitter.finagle.netty4.pushsession.Netty4PushTransporter
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.util.Duration
import com.twitter.util.Future
import java.net.SocketAddress

object PushStringClient {

  val protocolLibrary = StringClient.protocolLibrary

  case class Client(
    stack: Stack[ServiceFactory[String, String]] = StackClient.newStack,
    params: Stack.Params = Stack.Params.empty + ProtocolLibrary(protocolLibrary),
    appendDelimiter: Boolean = true)
      extends PushStackClient[String, String, Client] {
    protected def copy1(
      stack: Stack[ServiceFactory[String, String]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = String
    protected type Out = String
    protected type SessionT = PipeliningClientPushSession[String, String]

    protected def newPushTransporter(sa: SocketAddress): PushTransporter[String, String] = {
      val init = if (appendDelimiter) StringClientPipeline else NoDelimStringPipeline
      Netty4PushTransporter.raw[String, String](init, sa, params)
    }

    protected def newSession(handle: PushChannelHandle[String, String]): Future[SessionT] =
      Future.value(
        new PipeliningClientPushSession[String, String](
          handle,
          Duration.Top,
          DefaultTimer
        )
      )

    protected def toService(session: SessionT): Future[Service[String, String]] =
      Future.value(session.toService)
  }

  val client: Client = Client()
}
