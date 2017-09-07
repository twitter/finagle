package com.twitter.finagle.client

import com.twitter.finagle.{ChannelClosedException, Service, ServiceFactory, Stack, Status}
import com.twitter.finagle.client.StringClient.{NoDelimStringPipeline, StringClientPipeline}
import com.twitter.finagle.exp.pushsession.{
  PushChannelHandle,
  PushSession,
  PushStackClient,
  PushTransporter
}
import com.twitter.finagle.netty4.exp.pushsession.Netty4PushTransporter
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Return, Throw, Time}
import java.net.InetSocketAddress

object PushStringClient {

  val protocolLibrary = StringClient.protocolLibrary

  // With a few tests this should serve well as the simple pipelining client implementation
  protected final class SimpleClientSession[In, Out](handle: PushChannelHandle[In, Out])
      extends PushSession[In, Out](handle) { self =>

    private[this] val logger = Logger.get
    private[this] val queue = new java.util.ArrayDeque[Promise[In]]()
    @volatile private[this] var running: Boolean = true

    handle.onClose.respond { result =>
      if (running) handle.serialExecutor.execute(new Runnable {
        def run(): Unit = result match {
          case Return(_) => handleShutdown(None)
          case Throw(t) => handleShutdown(Some(t))
        }
      })
    }

    def receive(message: In): Unit = if (running) {
      val p = queue.poll()
      if (p != null) p.setValue(message)
      else
        handleShutdown(
          Some(new IllegalStateException("Received response with no corresponding request: " + message))
        )
    }

    def status: Status = {
      if (!running) Status.Closed
      else handle.status
    }

    def close(deadline: Time): Future[Unit] = handle.close(deadline)

    def toService: Service[Out, In] = new Service[Out, In] {
      def apply(request: Out): Future[In] = {
        val p = Promise[In]
        handle.serialExecutor.execute(new Runnable { def run(): Unit = handleDispatch(request, p) })
        p
      }

      override def close(deadline: Time): Future[Unit] = self.close(deadline)

      override def status: Status = self.status
    }

    // All shutdown pathways should funnel through this method
    private[this] def handleShutdown(cause: Option[Throwable]): Unit =
      if (running) {
        running = false
        cause.foreach(logger.info(_, "Session closing with exception"))
        close()
        val exc = cause.getOrElse(new ChannelClosedException(handle.remoteAddress))

        // Clear the queue.
        while (!queue.isEmpty) {
          queue.poll().setException(exc)
        }
      }

    private[this] def handleDispatch(request: Out, p: Promise[In]): Unit = {
      if (!running) p.setException(new ChannelClosedException(handle.remoteAddress))
      else {
        queue.offer(p)
        handle.sendAndForget(request)
      }
    }
  }

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
    protected type SessionT = SimpleClientSession[String, String]

    protected def newPushTransporter(ia: InetSocketAddress): PushTransporter[String, String] = {
      val init = if (appendDelimeter) StringClientPipeline else NoDelimStringPipeline
      Netty4PushTransporter.raw[String, String](init, ia, params)
    }

    protected def newSession(handle: PushChannelHandle[String, String]): Future[SessionT] =
      Future.value(new SimpleClientSession(handle))

    protected def toService(session: SessionT): Future[Service[String, String]] =
      Future.value(session.toService)
  }

  val client: Client = Client()
}
