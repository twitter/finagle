package com.twitter.finagle

import com.twitter.finagle
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.{GenSerialClientDispatcher, StalledPipelineTimeout}
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.param.{
  ExceptionStatsHandler => _,
  Monitor => _,
  ResponseClassifier => _,
  Tracer => _,
  _
}
import com.twitter.finagle.redis.exp.RedisPool
import com.twitter.finagle.redis.protocol.{Command, Reply, StageTransport}
import com.twitter.finagle.service.{ResponseClassifier, RetryBudget}
import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.io.Buf
import com.twitter.util.{Duration, Monitor}
import java.net.SocketAddress

trait RedisRichClient { self: Client[Command, Reply] =>

  def newRichClient(dest: String): redis.Client =
    redis.Client(newClient(dest))

  def newRichClient(dest: Name, label: String): redis.Client =
    redis.Client(newClient(dest, label))

  def newSentinelClient(dest: String): redis.SentinelClient =
    redis.SentinelClient(newClient(dest))

  def newSentinelClient(dest: Name, label: String): redis.SentinelClient =
    redis.SentinelClient(newClient(dest, label))

  def newTransactionalClient(dest: String): redis.TransactionalClient =
    redis.TransactionalClient(newClient(dest))

  def newTransactionalClient(dest: Name, label: String): redis.TransactionalClient =
    redis.TransactionalClient(newClient(dest, label))

  def newClusterClient(dest: String): redis.ClusterClient =
    redis.ClusterClient(newClient(dest))

  def newClusterClient(dest: Name, label: String): redis.ClusterClient =
    redis.ClusterClient(newClient(dest, label))
}

object Redis extends Client[Command, Reply] with RedisRichClient {

  object Client {

    /**
     * Default stack parameters used for redis client.
     */
    private val params: Stack.Params = StackClient.defaultParams +
      param.ProtocolLibrary("redis")

    /**
     * A default client stack which supports the pipelined redis client.
     */
    private val stack: Stack[ServiceFactory[Command, Reply]] = StackClient.newStack
      .insertBefore(DefaultPool.Role, RedisPool.module)
  }

  case class Client(
    stack: Stack[ServiceFactory[Command, Reply]] = Client.stack,
    params: Stack.Params = Client.params
  ) extends StdStackClient[Command, Reply, Client]
      with WithDefaultLoadBalancer[Client]
      with RedisRichClient {

    protected def copy1(
      stack: Stack[ServiceFactory[Command, Reply]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf
    protected type Context = TransportContext

    protected def newTransporter(addr: SocketAddress): Transporter[In, Out, Context] =
      Netty4Transporter.framedBuf(None /* no Framer */, addr, params)

    protected def newDispatcher(transport: Transport[In, Out] {
      type Context <: Client.this.Context
    }): Service[Command, Reply] =
      RedisPool.newDispatcher(
        new StageTransport(transport),
        params[finagle.param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope),
        params[StalledPipelineTimeout].timeout
      )

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
    override def withRetryBackoff(backoff: Stream[Duration]): Client =
      super.withRetryBackoff(backoff)

    override def withStack(stack: Stack[ServiceFactory[Command, Reply]]): Client =
      super.withStack(stack)
    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
    override def filtered(filter: Filter[Command, Reply, Command, Reply]): Client =
      super.filtered(filter)
  }

  def client: Redis.Client = Client()

  def newClient(dest: Name, label: String): ServiceFactory[Command, Reply] =
    client.newClient(dest, label)

  def newService(dest: Name, label: String): Service[Command, Reply] =
    client.newService(dest, label)
}
