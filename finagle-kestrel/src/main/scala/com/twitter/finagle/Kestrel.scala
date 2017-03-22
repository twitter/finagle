package com.twitter.finagle

import _root_.java.net.SocketAddress
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.client.{DefaultPool, StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.{GenSerialClientDispatcher, SerialClientDispatcher}
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.memcached.protocol.text.client.ClientTransport
import com.twitter.finagle.memcached.protocol.text.transport.Netty4ClientFramer
import com.twitter.finagle.param.{Stats, ExceptionStatsHandler => _, Monitor => _, ResponseClassifier => _, Tracer => _, _}
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.tracing.{ClientRequestTracingFilter, Tracer}
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Duration, Monitor}

object Kestrel {

  /**
   * A client for Kestrel, which operates over the memcached protocol.
   */
  object Client {
    val defaultParams: Stack.Params = StackClient.defaultParams + ProtocolLibrary("kestrel")

    def newStack: Stack[ServiceFactory[Command, Response]] =
      StackClient.newStack
        .replace(DefaultPool.Role, SingletonPool.module[Command, Response])
        .prepend(Filter.canStackFromFac.toStackable(Stack.Role("KestrelTracingFilter"), KestrelTracingFilter))
  }

  case class Client(
      stack: Stack[ServiceFactory[Command, Response]] = Client.newStack,
      params: Stack.Params = Client.defaultParams)
    extends StdStackClient[Command, Response, Client]
    with WithDefaultLoadBalancer[Client] {

    override protected def copy1(
      stack: Stack[ServiceFactory[Command, Response]] = this.stack,
      params: Params = this.params
    ): Client = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf

    protected def newTransporter(addr: SocketAddress): Transporter[In, Out] =
      Netty4Transporter.raw(Netty4ClientFramer, addr, params)

    protected def newDispatcher(transport: Transport[In, Out]): Service[Command, Response] = {
      new SerialClientDispatcher(
        new ClientTransport[Command, Response](
          new CommandToEncoding,
          new DecodingToResponse,
          transport),
        params[Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
      )
    }

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withLoadBalancer: DefaultLoadBalancingParams[Client] =
      new DefaultLoadBalancingParams(this)
    override val withTransport: ClientTransportParams[Client] =
      new ClientTransportParams(this)
    override val withSession: ClientSessionParams[Client] =
      new ClientSessionParams(this)
    override val withSessionQualifier: SessionQualificationParams[Client] =
      new SessionQualificationParams(this)
    override val withAdmissionControl: ClientAdmissionControlParams[Client] =
      new ClientAdmissionControlParams(this)

    override def withLabel(label: String): Client = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Client =
      super.withStatsReceiver(statsReceiver)
    override def withMonitor(monitor: Monitor): Client = super.withMonitor(monitor)
    override def withTracer(tracer: Tracer): Client = super.withTracer(tracer)
    override def withExceptionStatsHandler(exceptionStatsHandler: ExceptionStatsHandler): Client =
      super.withExceptionStatsHandler(exceptionStatsHandler)
    override def withRequestTimeout(timeout: Duration): Client = super.withRequestTimeout(timeout)
    override def withResponseClassifier(responseClassifier: ResponseClassifier): Client =
      super.withResponseClassifier(responseClassifier)
    override def withRetryBudget(budget: RetryBudget): Client = super.withRetryBudget(budget)
    override def withRetryBackoff(backoff: Stream[Duration]): Client = super.withRetryBackoff(backoff)

    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
    override def filtered(filter: Filter[Command, Response, Command, Response]): Client =
      super.filtered(filter)
  }

  val client: Kestrel.Client = Client()

}

/**
 * Adds tracing information for each kestrel request.
 * Including command name, when request was sent and when it was received.
 */
private[finagle] object KestrelTracingFilter extends ClientRequestTracingFilter[Command, Response] {
  val serviceName = "kestrel"
  def methodName(req: Command): String = req.name
}
