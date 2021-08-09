package com.twitter.finagle

import com.twitter.finagle
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.{ClientDispatcher, StalledPipelineTimeout}
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.param.{
  ExceptionStatsHandler => _,
  Monitor => _,
  ResponseClassifier => _,
  Tracer => _,
  _
}
import com.twitter.finagle.redis.RedisPartitioningService
import com.twitter.finagle.redis.exp.{ConnectionInitCommand, RedisPool}
import com.twitter.finagle.redis.filter.{RedisLoggingFilter, RedisTracingFilter}
import com.twitter.finagle.redis.param.{Database, Password}
import com.twitter.finagle.redis.protocol.{Command, Reply, StageTransport}
import com.twitter.finagle.service.{ResponseClassifier, RetryBudget}
import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.io.Buf
import com.twitter.util.{Duration, FuturePool, Monitor}
import java.net.SocketAddress
import java.util.concurrent.ExecutorService

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

  def newPartitionedClient(dest: Name, label: String): redis.PartitionedClient =
    redis.PartitionedClient.apply(
      Redis.partitionedClient.newClient(dest, label)
    )

  def newPartitionedClient(dest: String): redis.PartitionedClient =
    redis.PartitionedClient.apply(
      Redis.partitionedClient.newClient(dest)
    )
}

object Redis extends Client[Command, Reply] with RedisRichClient {

  object Client {

    /**
     * Default stack parameters used for redis client.
     */
    private def params: Stack.Params = StackClient.defaultParams +
      param.ProtocolLibrary("redis")

    /**
     * A default client stack which supports the pipelined redis client.
     */
    private val stack: Stack[ServiceFactory[Command, Reply]] = StackClient.newStack
      .insertBefore(DefaultPool.Role, RedisPool.module)
      .insertAfter(StackClient.Role.prepConn, ConnectionInitCommand.module)
      .replace(StackClient.Role.protoTracing, RedisTracingFilter.module)
      .insertBefore(StackClient.Role.protoTracing, RedisLoggingFilter.module)

    private[finagle] val hashRingStack: Stack[ServiceFactory[Command, Reply]] =
      stack.insertAfter(BindingFactory.role, RedisPartitioningService.module)
  }

  case class Client(
    stack: Stack[ServiceFactory[Command, Reply]] = Client.stack,
    params: Stack.Params = Client.params)
      extends StdStackClient[Command, Reply, Client]
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

    protected def newDispatcher(
      transport: Transport[In, Out] {
        type Context <: Client.this.Context
      }
    ): Service[Command, Reply] =
      RedisPool.newDispatcher(
        new StageTransport(transport),
        params[finagle.param.Stats].statsReceiver.scope(ClientDispatcher.StatsScope),
        params[StalledPipelineTimeout].timeout
      )

    /**
     * Database to use when this client establishes a new connection.
     */
    def withDatabase(db: Int): Client =
      configured(Database(Some(db)))

    /**
     * Password to use when authenticating a new connection.
     */
    def withPassword(password: Buf): Client =
      configured(Password(Some(password)))

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
    override def withRetryBackoff(backoff: Backoff): Client =
      super.withRetryBackoff(backoff)

    override def withStack(stack: Stack[ServiceFactory[Command, Reply]]): Client =
      super.withStack(stack)
    override def withStack(
      fn: Stack[ServiceFactory[Command, Reply]] => Stack[ServiceFactory[Command, Reply]]
    ): Client =
      super.withStack(fn)
    override def withExecutionOffloaded(executor: ExecutorService): Client =
      super.withExecutionOffloaded(executor)
    override def withExecutionOffloaded(pool: FuturePool): Client =
      super.withExecutionOffloaded(pool)
    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
    override def filtered(filter: Filter[Command, Reply, Command, Reply]): Client =
      super.filtered(filter)
  }

  def client: Redis.Client = Client()

  def partitionedClient: Redis.Client =
    client.withStack(Client.hashRingStack)

  def newClient(dest: Name, label: String): ServiceFactory[Command, Reply] =
    client.newClient(dest, label)

  def newService(dest: Name, label: String): Service[Command, Reply] =
    client.newService(dest, label)
}
