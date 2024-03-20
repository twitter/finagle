package com.twitter.finagle

import com.twitter.conversions.DurationOps._
import com.twitter.finagle
import com.twitter.hashing
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.dispatch.StalledPipelineTimeout
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.liveness.FailureAccrualPolicy
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.memcached.exp.LocalMemcached
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.compressing.WithCompressionScheme
import com.twitter.finagle.memcached.compressing.scheme.CompressionScheme
import com.twitter.finagle.memcached.partitioning.MemcachedPartitioningService
import com.twitter.finagle.memcached.protocol.text.server.ServerTransport
import com.twitter.finagle.memcached.protocol.text.transport.MemcachedNetty4ClientPipelineInit
import com.twitter.finagle.memcached.protocol.text.transport.Netty4ServerFramer
import com.twitter.finagle.memcached.protocol.Command
import com.twitter.finagle.memcached.protocol.Response
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.netty4.pushsession.Netty4PushTransporter
import com.twitter.finagle.param.{
  ExceptionStatsHandler => _,
  Monitor => _,
  ResponseClassifier => _,
  Tracer => _,
  _
}
import com.twitter.finagle.partitioning.param.KeyHasher
import com.twitter.finagle.partitioning.param.WithPartitioningStrategy
import com.twitter.finagle.pool.BalancingPool
import com.twitter.finagle.pushsession.PipeliningClientPushSession
import com.twitter.finagle.pushsession.PushChannelHandle
import com.twitter.finagle.pushsession.PushStackClient
import com.twitter.finagle.pushsession.PushTransporter
import com.twitter.finagle.server.Listener
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.server.StdStackServer
import com.twitter.finagle.service._
import com.twitter.finagle.stats.ExceptionStatsHandler
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.ClientDestTracingFilter
import com.twitter.finagle.tracing.ClientTracingFilter
import com.twitter.finagle.tracing.TraceInitializerFilter
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.finagle.util.DefaultLogger
import com.twitter.io.Buf
import com.twitter.util._
import com.twitter.util.registry.GlobalRegistry
import java.net.SocketAddress
import java.util.concurrent.ExecutorService

/**
 * Factory methods to build a finagle-memcached client.
 *
 * @define partitioned
 *
 * Constructs a memcached.Client that dispatches requests over `dest`. When
 * `dest` resolves to multiple hosts, the hosts are hashed across a ring with
 * key affinity. The key hashing algorithm can be configured via the `withKeyHasher`
 * method on `Memcached.client`. Failing hosts can be ejected from the
 * hash ring if `withEjectFailedHost` is set to true. Note, the current
 * implementation only supports bound [[com.twitter.finagle.Name Names]].
 *
 * @define notpartioned
 * If LoadBalancedTwemcacheClient is used to create a client a key hasher won't be used
 * and instead a load balancing algorithm will be used that doesn't account for keys. Some settings
 * related to configuring client side hashing will be ignored
 *
 * @define label
 * Argument `label` is used to assign a label to this client.
 * The label is used to scope stats, etc.
 */
trait MemcachedRichClient { self: finagle.Client[Command, Response] =>

  /** $partitioned $label */
  def newRichClient(dest: Name, label: String): memcached.Client =
    newTwemcacheClient(dest, label)

  /** $partitioned */
  def newRichClient(dest: String): memcached.Client = {
    val (n, l) = evalLabeledDest(dest)
    newTwemcacheClient(n, l)
  }

  /** $partitioned $label */
  def newTwemcacheClient(dest: Name, label: String): TwemcacheClient

  /** $partitioned */
  def newTwemcacheClient(dest: String): TwemcacheClient = {
    val (n, l) = evalLabeledDest(dest)
    newTwemcacheClient(n, l)
  }

  /** $notpartioned $label */
  def newLoadBalancedTwemcacheClient(dest: Name, label: String): TwemcacheClient

  /** $notpartioned */
  def newLoadBalancedTwemcacheClient(dest: String): TwemcacheClient = {
    val (n, l) = evalLabeledDest(dest)
    newLoadBalancedTwemcacheClient(n, l)
  }

  private def evalLabeledDest(dest: String): (Name, String) = {
    val _dest = if (LocalMemcached.enabled) {
      Memcached.Client.mkDestination("localhost", LocalMemcached.port)
    } else dest
    Resolver.evalLabeled(_dest)
  }
}

/**
 * Stack based Memcached client.
 *
 * For example, a default client can be built through:
 *
 * @example {{{
 *   val client = Memcached.newRichClient(dest)
 * }}}
 *
 * If you want to provide more finely tuned configurations:
 * @example {{{
 *   val client =
 *     Memcached.client
 *       .withEjectFailedHost(true)
 *       .withTransport.connectTimeout(100.milliseconds))
 *       .withRequestTimeout(10.seconds)
 *       .withSession.acquisitionTimeout(20.seconds)
 *       .newRichClient(dest, "memcached_client")
 * }}}
 */
object Memcached extends finagle.Client[Command, Response] with finagle.Server[Command, Response] {

  object Client {

    private[Memcached] val ProtocolLibraryName = "memcached"

    /**
     * We use 10 consecutive errors as we have experience that per connection rps within a client
     * is a lot lower than the overall rps generated by a service and memcached backends do not
     * tend to show transient errors. Clients should backoff early and long enough to let a failing
     * backend removed from the cache ring.
     * per_conn_rps = total_rps / (number_of_clients * number_of_backends * number_of_conn_per_client)
     */
    private val defaultFailureAccrualPolicy = () =>
      FailureAccrualPolicy.consecutiveFailures(10, Backoff.const(30.seconds))

    /**
     * To prevent an ever growing pipeline which can cause increased memory pressure on the client,
     * we set a limit to the number of pending requests allowed. Requests overflowing this limit
     * will return a RejectedExecutionException.
     */
    private val defaultPendingRequestLimit = Some(100)

    private val defaultNumConnections: Int = 2

    /**
     * Default stack parameters used for memcached client. We change the
     * load balancer to `p2cPeakEwma` as we have experience improved tail
     * latencies when coupled with the pipelining dispatcher.
     */
    private def params: Stack.Params = StackClient.defaultParams +
      FailureAccrualFactory.Param(defaultFailureAccrualPolicy) +
      FailFastFactory.FailFast(false) +
      LoadBalancerFactory.Param(Balancers.p2cPeakEwma()) +
      LoadBalancerFactory.ReplicateAddresses(defaultNumConnections) +
      PendingRequestFilter.Param(limit = defaultPendingRequestLimit) +
      ProtocolLibrary(ProtocolLibraryName)

    /**
     * A default client stack which supports the pipelined memcached client.
     * The `ConcurrentLoadBalancerFactory` load balances over a small set of
     * duplicate endpoints to eliminate head of line blocking. Each endpoint
     * has a single pipelined connection.
     */
    private val stack: Stack[ServiceFactory[Command, Response]] = StackClient.newStack
      .replace(
        StackClient.Role.pool,
        BalancingPool.module[Command, Response](allowInterrupts = true))
      .replace(StackClient.Role.protoTracing, MemcachedTracingFilter.memcachedTracingModule)
      // we place this module at a point where the endpoint is resolved in the stack.
      .insertBefore(ClientDestTracingFilter.role, MemcachedTracingFilter.shardIdTracingModule)
      .prepend(CompressingMemcachedFilter.memcachedCompressingModule)

    /**
     * The memcached client should be using fixed hosts that do not change
     * IP addresses. Force usage of the FixedInetResolver to prevent spurious
     * DNS lookups and polling.
     */
    def mkDestination(hostName: String, port: Int): String =
      s"${FixedInetResolver.scheme}!$hostName:$port"

    /*
     * We are migrating the Memcached client to be push-based. To facilitate a gradual rollout,
     * The PushClient and NonPushClient provide endpointer service factories (`enptr`) for the
     * [[Memcached.Client]] to use. In the future, the client will be able to be toggled to use
     * the push or non-push underlying client and its endpointer implementation. Currently,
     * the client is hard-coded to use the non-push underlying client.
     */

  }

  private[finagle] def registerClient(label: String, hasher: String): Unit = {
    GlobalRegistry.get.put(
      Seq(ClientRegistry.registryName, Client.ProtocolLibraryName, label, "key_hasher"),
      hasher
    )
  }

  /**
   * A memcached client with support for pipelined requests, consistent hashing,
   * and per-node load-balancing.
   */
  case class Client(
    stack: Stack[ServiceFactory[Command, Response]] = Client.stack,
    params: Stack.Params = Client.params)
      extends PushStackClient[Command, Response, Client]
      with WithPartitioningStrategy[Client]
      with WithCompressionScheme[Client]
      with MemcachedRichClient {

    protected type In = Response
    protected type Out = Command
    protected type SessionT = PipeliningClientPushSession[Response, Command]

    protected def newPushTransporter(sa: SocketAddress): PushTransporter[Response, Command] =
      Netty4PushTransporter.raw(MemcachedNetty4ClientPipelineInit, sa, params)

    protected def newSession(handle: PushChannelHandle[Response, Command]): Future[SessionT] = {
      Future.value(
        new PipeliningClientPushSession[Response, Command](
          handle,
          params[StalledPipelineTimeout].timeout,
          params[finagle.param.Timer].timer
        )
      )
    }

    protected def toService(session: SessionT): Future[Service[Command, Response]] =
      Future.value(session.toService)

    protected def copy1(
      stack: Stack[ServiceFactory[Command, Response]],
      params: Stack.Params
    ): Client = copy(stack, params)

    def newTwemcacheClient(dest: Name, label: String): TwemcacheClient = {
      val destination = if (LocalMemcached.enabled) {
        Resolver.eval(Client.mkDestination("localhost", LocalMemcached.port))
      } else dest

      val label0 = if (label == "") params[Label].label else label

      val KeyHasher(hasher) = params[KeyHasher]
      registerClient(label0, hasher.toString)

      def partitionAwareFinagleClient() = {
        DefaultLogger.fine(s"Using the new partitioning finagle client for memcached: $destination")
        val rawClient: Service[Command, Response] = {
          val stk = stack
            .insertAfter(
              BindingFactory.role,
              MemcachedPartitioningService.module
            )
            // We want this to go after the MemcachedPartitioningService so that we can get individual
            // spans for fanout requests. It's currently at protoTracing, so we remove it to re-add below
            .remove(MemcachedTracingFilter.memcachedTracingModule.role)
            .remove(ClientTracingFilter.role)
            .insertAfter(
              MemcachedPartitioningService.role,
              MemcachedTracingFilter.memcachedTracingModule)
            .replace(
              TraceInitializerFilter.role,
              TraceInitializerFilter.clientModule[Command, Response](fanout = true))
            .insertAfter(
              MemcachedTracingFilter.memcachedTracingModule.role,
              ClientTracingFilter.module[Command, Response])
          withStack(stk).newService(destination, label0)
        }
        TwemcacheClient(rawClient)
      }

      destination match {
        case Name.Bound(va) =>
          partitionAwareFinagleClient()
        case Name.Path(_path: Path) =>
          partitionAwareFinagleClient()
        case n =>
          throw new IllegalArgumentException(
            s"Memcached client only supports Bound Names or Name.Path, was: $n"
          )
      }
    }

    def newLoadBalancedTwemcacheClient(
      dest: Name,
      label: String
    ): TwemcacheClient = {
      val destination = if (LocalMemcached.enabled) {
        Resolver.eval(Client.mkDestination("localhost", LocalMemcached.port))
      } else dest

      val label0 = if (label == "") params[Label].label else label

      TwemcacheClient(newService(destination, label0))
    }

    /**
     * Configures the number of concurrent `connections` a single endpoint has.
     * The connections are load balanced over which allows the pipelined client to
     * avoid head-of-line blocking and reduce its latency.
     *
     * We've empirically found that two is a good default for this, but it can be
     * increased at the cost of additional connection overhead.
     */
    def connectionsPerEndpoint(connections: Int): Client =
      configured(LoadBalancerFactory.ReplicateAddresses(connections))

    override val withTransport: ClientTransportParams[Client] =
      new ClientTransportParams(this)
    override val withSession: ClientSessionParams[Client] =
      new ClientSessionParams(this)
    override val withAdmissionControl: ClientAdmissionControlParams[Client] =
      new ClientAdmissionControlParams(this)
    override val withSessionQualifier: SessionQualificationParams[Client] =
      new SessionQualificationParams(this)

    override def withEjectFailedHost(eject: Boolean): Client = super.withEjectFailedHost(eject)
    override def withKeyHasher(hasher: hashing.KeyHasher): Client = super.withKeyHasher(hasher)
    override def withNumReps(reps: Int): Client = super.withNumReps(reps)

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

    override def withStack(stack: Stack[ServiceFactory[Command, Response]]): Client =
      super.withStack(stack)
    override def withStack(
      fn: Stack[ServiceFactory[Command, Response]] => Stack[ServiceFactory[Command, Response]]
    ): Client =
      super.withStack(fn)
    override def withExecutionOffloaded(executor: ExecutorService): Client =
      super.withExecutionOffloaded(executor)
    override def withExecutionOffloaded(pool: FuturePool): Client =
      super.withExecutionOffloaded(pool)
    override def configured[P](psp: (P, Stack.Param[P])): Client = super.configured(psp)
    override def filtered(filter: Filter[Command, Response, Command, Response]): Client =
      super.filtered(filter)

    override def withCompressionScheme(scheme: CompressionScheme): Client = super
      .withCompressionScheme(scheme)
  }

  def client: Memcached.Client = Client()

  def newClient(dest: Name, label: String): ServiceFactory[Command, Response] =
    client.newClient(dest, label)

  def newService(dest: Name, label: String): Service[Command, Response] =
    client.newService(dest, label)

  object Server {

    /**
     * Default stack parameters used for memcached server.
     */
    private def params: Stack.Params = StackServer.defaultParams +
      ProtocolLibrary("memcached")
  }

  /**
   * A Memcached server that should be used only for testing
   */
  case class Server(
    stack: Stack[ServiceFactory[Command, Response]] = StackServer.newStack,
    params: Stack.Params = Server.params)
      extends StdStackServer[Command, Response, Server] {

    protected def copy1(
      stack: Stack[ServiceFactory[Command, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf
    protected type Context = TransportContext

    protected def newListener(): Listener[In, Out, TransportContext] =
      Netty4Listener[Buf, Buf](Netty4ServerFramer, params)

    protected def newDispatcher(
      transport: Transport[In, Out] { type Context <: Server.this.Context },
      service: Service[Command, Response]
    ): Closable = new SerialServerDispatcher(new ServerTransport(transport), service)

    // Java-friendly forwarders
    //See https://issues.scala-lang.org/browse/SI-8905
    override val withAdmissionControl: ServerAdmissionControlParams[Server] =
      new ServerAdmissionControlParams(this)
    override val withSession: ServerSessionParams[Server] =
      new ServerSessionParams(this)
    override val withTransport: ServerTransportParams[Server] =
      new ServerTransportParams(this)

    override def withLabel(label: String): Server = super.withLabel(label)
    override def withStatsReceiver(statsReceiver: StatsReceiver): Server =
      super.withStatsReceiver(statsReceiver)
    override def withMonitor(monitor: Monitor): Server = super.withMonitor(monitor)
    override def withTracer(tracer: Tracer): Server = super.withTracer(tracer)
    override def withExceptionStatsHandler(exceptionStatsHandler: ExceptionStatsHandler): Server =
      super.withExceptionStatsHandler(exceptionStatsHandler)
    override def withRequestTimeout(timeout: Duration): Server = super.withRequestTimeout(timeout)

    override def withStack(stack: Stack[ServiceFactory[Command, Response]]): Server =
      super.withStack(stack)

    override def withStack(
      fn: Stack[ServiceFactory[Command, Response]] => Stack[ServiceFactory[Command, Response]]
    ): Server =
      super.withStack(fn)
    override def withExecutionOffloaded(executor: ExecutorService): Server =
      super.withExecutionOffloaded(executor)
    override def withExecutionOffloaded(pool: FuturePool): Server =
      super.withExecutionOffloaded(pool)
    override def configured[P](psp: (P, Stack.Param[P])): Server = super.configured(psp)
  }

  def server: Memcached.Server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[Command, Response]): ListeningServer =
    server.serve(addr, service)
}
