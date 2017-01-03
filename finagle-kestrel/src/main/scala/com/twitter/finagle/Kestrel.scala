package com.twitter.finagle

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.client.{DefaultPool, StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.{GenSerialClientDispatcher, SerialClientDispatcher}
import com.twitter.finagle.kestrel.Toggles
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.memcached.protocol.text.client.ClientTransport
import com.twitter.finagle.memcached.protocol.text.transport.{Netty3ClientFramer, Netty4ClientFramer}
import com.twitter.finagle.param.{Stats, ExceptionStatsHandler => _, Monitor => _, ResponseClassifier => _, Tracer => _, _}
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.toggle.Toggle
import com.twitter.finagle.tracing.{ClientRequestTracingFilter, Tracer}
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Duration, Monitor}

object Kestrel {

  object param {

    /**
     * Configure the [[Transporter]] implementation used by Kestrel.
     */
    case class KestrelImpl(
      transporter: Stack.Params => Transporter[Buf, Buf]){

      def mk(): (KestrelImpl, Stack.Param[KestrelImpl]) =
        (this, KestrelImpl.param)
    }

    object KestrelImpl {
      /**
       * A [[KestrelImpl]] that uses netty3 as the underlying I/O multiplexer.
       */
      val Netty3 = KestrelImpl(
        params => Netty3Transporter[Buf, Buf](Netty3ClientFramer, params))

      /**
       * A [[KestrelImpl]] that uses netty4 as the underlying I/O multiplexer.
       *
       * @note Important! This is experimental and not yet tested in production!
       */
      val Netty4 = KestrelImpl(
        params => Netty4Transporter[Buf, Buf](Netty4ClientFramer, params))

      private[this] val UseNetty4ToggleId: String = "com.twitter.finagle.kestrel.UseNetty4"
      private[this] val netty4Toggle: Toggle[Int] = Toggles(UseNetty4ToggleId)
      private[this] def useNetty4: Boolean = netty4Toggle(ServerInfo().id.hashCode)

      implicit val param: Stack.Param[KestrelImpl] = Stack.Param(
         if (useNetty4) Netty4
         else Netty3
      )
    }
  }

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

    protected def newTransporter(): Transporter[In, Out] =
      params[param.KestrelImpl].transporter(params)

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