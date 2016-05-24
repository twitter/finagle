package com.twitter.finagle

import _root_.java.net.SocketAddress
import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle.client.{ClientRegistry, DefaultPool, StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.{GenSerialClientDispatcher, SerialServerDispatcher, PipeliningDispatcher}
import com.twitter.finagle.loadbalancer.{Balancers, ConcurrentLoadBalancerFactory, LoadBalancerFactory}
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.exp.LocalMemcached
import com.twitter.finagle.memcached.protocol.text.{MemcachedClientPipelineFactory, MemcachedServerPipelineFactory}
import com.twitter.finagle.memcached.protocol.{Command, Response, RetrievalCommand, Values}
import com.twitter.finagle.netty3.{BufChannelBuffer, Netty3Listener, Netty3Transporter}
import com.twitter.finagle.param.{Monitor => _, ResponseClassifier => _, ExceptionStatsHandler => _, Tracer => _, _}
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server.{Listener, StackServer, StdStackServer}
import com.twitter.finagle.service._
import com.twitter.finagle.service.exp.FailureAccrualPolicy
import com.twitter.finagle.stats.{StatsReceiver, ExceptionStatsHandler}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.hashing
import com.twitter.io.Buf
import com.twitter.util.{Closable, Duration, Future, Monitor}
import com.twitter.util.registry.GlobalRegistry
import scala.collection.mutable

/**
 * Defines a [[Tracer]] that understands the finagle-memcached request type.
 * This is installed as the `ClientTracingFilter` in the default stack.
 */
private[finagle] object MemcachedTraceInitializer {
  object Module extends Stack.Module1[param.Tracer, ServiceFactory[Command, Response]] {
    val role = TraceInitializerFilter.role
    val description = "Initialize traces for the client and record hits/misses"
    def make(_tracer: param.Tracer, next: ServiceFactory[Command, Response]) = {
      val param.Tracer(tracer) = _tracer
      val filter = new Filter(tracer)
      filter andThen next
    }
  }

  class Filter(tracer: Tracer) extends SimpleFilter[Command, Response] {
    def apply(command: Command, service: Service[Command, Response]): Future[Response] =
      Trace.letTracerAndNextId(tracer) {
        val response = service(command)
        Trace.recordRpc(command.name)
        command match {
          case command: RetrievalCommand if Trace.isActivelyTracing =>
            response onSuccess {
              case Values(vals) =>
                val cmd = command.asInstanceOf[RetrievalCommand]
                val misses = mutable.Set.empty[String]
                cmd.keys foreach { case Buf.Utf8(key) => misses += key }
                vals foreach { value =>
                  val Buf.Utf8(key) = value.key
                  Trace.recordBinary(key, "Hit")
                  misses.remove(key)
                }
                misses foreach { Trace.recordBinary(_, "Miss") }
              case _ =>
            }
          case _ =>
        }
        response
      }
  }
}

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
 * @define label
 *
 * Argument `label` is used to assign a label to this client.
 * The label is used to scope stats, etc.
 */
trait MemcachedRichClient { self: finagle.Client[Command, Response] =>
  /** $partitioned $label */
  def newRichClient(dest: Name, label: String): memcached.Client =
    newTwemcacheClient(dest, label)

  /** $partitioned */
  def newRichClient(dest: String): memcached.Client = {
    val (n, l) = Resolver.evalLabeled(dest)
    newTwemcacheClient(n, l)
  }

  /** $partitioned $label */
  def newTwemcacheClient(dest: Name, label: String): TwemcacheClient

  /** $partitioned */
  def newTwemcacheClient(dest: String): TwemcacheClient = {
    val (n, l) = Resolver.evalLabeled(dest)
    newTwemcacheClient(n, l)
  }
}

/**
 * Stack based Memcached client.
 *
 * For example, a default client can be built through:
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
object Memcached extends finagle.Client[Command, Response]
  with finagle.Server[Command, Response] {

  /**
   * Memcached specific stack params.
   */
  object param {
    case class EjectFailedHost(v: Boolean) {
      def mk(): (EjectFailedHost, Stack.Param[EjectFailedHost]) =
        (this, EjectFailedHost.param)
    }

    object EjectFailedHost {
      implicit val param = Stack.Param(EjectFailedHost(false))
    }

    case class KeyHasher(hasher: hashing.KeyHasher) {
      def mk(): (KeyHasher, Stack.Param[KeyHasher]) =
        (this, KeyHasher.param)
    }

    object KeyHasher {
      implicit val param = Stack.Param(KeyHasher(hashing.KeyHasher.KETAMA))
    }

    case class NumReps(reps: Int) {
      def mk(): (NumReps, Stack.Param[NumReps]) =
        (this, NumReps.param)
    }

    object NumReps {
      implicit val param = Stack.Param(NumReps(KetamaPartitionedClient.DefaultNumReps))
    }
  }

  object Client {

    private[Memcached] val ProtocolLibraryName = "memcached"

    private[this] val defaultFailureAccrualPolicy = () => FailureAccrualPolicy.consecutiveFailures(
      100, Backoff.const(1.second))

    /**
     * Default stack parameters used for memcached client. We change the
     * load balancer to `p2cPeakEwma` as we have experience improved tail
     * latencies when coupled with the pipelining dispatcher.
     */
    val defaultParams: Stack.Params = StackClient.defaultParams +
      FailureAccrualFactory.Param(defaultFailureAccrualPolicy) +
      FailFastFactory.FailFast(false) +
      LoadBalancerFactory.Param(Balancers.p2cPeakEwma()) +
      finagle.param.ProtocolLibrary(ProtocolLibraryName)

    /**
     * A default client stack which supports the pipelined memcached client.
     * The `ConcurrentLoadBalancerFactory` load balances over a small set of
     * duplicate endpoints to eliminate head of line blocking. Each endpoint
     * has a single pipelined connection.
     */
    def newStack: Stack[ServiceFactory[Command, Response]] = StackClient.newStack
      .replace(LoadBalancerFactory.role, ConcurrentLoadBalancerFactory.module[Command, Response])
      .replace(DefaultPool.Role, SingletonPool.module[Command, Response])
      .replace(ClientTracingFilter.role, MemcachedTraceInitializer.Module)

    /**
     * The memcached client should be using fixed hosts that do not change
     * IP addresses. Force usage of the FixedInetResolver to prevent spurious
     * DNS lookups and polling.
     */
    def mkDestination(hostName: String, port: Int): String =
      s"${FixedInetResolver.scheme}!$hostName:$port"
  }

  private[finagle] def registerClient(
    label: String,
    hasher: String,
    isPipelining: Boolean
  ): Unit = {
    GlobalRegistry.get.put(
      Seq(ClientRegistry.registryName, Client.ProtocolLibraryName, label, "is_pipelining"),
      isPipelining.toString)
    GlobalRegistry.get.put(
      Seq(ClientRegistry.registryName, Client.ProtocolLibraryName, label, "key_hasher"),
      hasher)
  }

  /**
   * A memcached client with support for pipelined requests, consistent hashing,
   * and per-node load-balancing.
   */
  case class Client(
      stack: Stack[ServiceFactory[Command, Response]] = Client.newStack,
      params: Stack.Params = Client.defaultParams)
    extends StdStackClient[Command, Response, Client]
    with WithConcurrentLoadBalancer[Client]
    with MemcachedRichClient {

    import Client.mkDestination

    protected def copy1(
      stack: Stack[ServiceFactory[Command, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = Command
    protected type Out = Response

    protected def newTransporter(): Transporter[In, Out] =
      Netty3Transporter(MemcachedClientPipelineFactory, params)

    protected def newDispatcher(transport: Transport[In, Out]): Service[Command, Response] =
      new PipeliningDispatcher(
        transport,
        params[finagle.param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)
      )

    def newTwemcacheClient(dest: Name, label: String): TwemcacheClient = {
      val _dest = if (LocalMemcached.enabled) {
        Resolver.eval(mkDestination("localhost", LocalMemcached.port))
      } else dest

      // Memcache only support Name.Bound names (TRFC-162).
      // See KetamaPartitionedClient for more details.
      val va = _dest match {
        case Name.Bound(va) => va
        case n =>
          throw new IllegalArgumentException(s"Memcached client only supports Bound Names, was: $n")
      }

      val finagle.param.Stats(sr) = params[finagle.param.Stats]
      val param.KeyHasher(hasher) = params[param.KeyHasher]
      val param.NumReps(numReps) = params[param.NumReps]

      registerClient(label, hasher.toString, isPipelining = true)

      val healthBroker = new Broker[NodeHealth]

      def newService(node: CacheNode): Service[Command, Response] = {
        val key = KetamaClientKey.fromCacheNode(node)
        val stk = stack.replace(FailureAccrualFactory.role,
          KetamaFailureAccrualFactory.module[Command, Response](key, healthBroker))
        withStack(stk).newService(mkDestination(node.host, node.port), label)
      }

      val group = CacheNodeGroup.fromVarAddr(va)
      val scopedSr = sr.scope(label)
      new KetamaPartitionedClient(group, newService, healthBroker, scopedSr, hasher, numReps)
        with TwemcachePartitionedClient
    }

    /**
     * Whether to eject cache host from the Ketama ring based on failure accrual.
     * By default, this is off. When turning on, keep the following caveat in
     * mind: ejection is based on local failure accrual, so your cluster may
     * get different views of the same cache host. With cache updates, this can
     * introduce inconsistency in cache data. In many cases, it's better to eject
     * cache host from a separate mechanism that's based on a global view.
     */
    def withEjectFailedHost(eject: Boolean): Client =
      configured(param.EjectFailedHost(eject))

    /**
     * Defines the hash function to use for partitioned clients when
     * mapping keys to partitions.
     */
    def withKeyHasher(hasher: hashing.KeyHasher): Client =
      configured(param.KeyHasher(hasher))

    /**
     * Duplicate each node across the hash ring according to `reps`.
     *
     * @see [[com.twitter.hashing.KetamaDistributor]] for more
     * details.
     */
    def withNumReps(reps: Int): Client =
      configured(param.NumReps(reps))

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withLoadBalancer: ConcurrentLoadBalancingParams[Client] =
      new ConcurrentLoadBalancingParams(this)
    override val withTransport: ClientTransportParams[Client] =
      new ClientTransportParams(this)
    override val withSession: SessionParams[Client] =
      new SessionParams(this)
    override val withAdmissionControl: ClientAdmissionControlParams[Client] =
      new ClientAdmissionControlParams(this)
    override val withSessionQualifier: SessionQualificationParams[Client] =
      new SessionQualificationParams(this)

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

  val client: Memcached.Client = Client()

  def newClient(dest: Name, label: String): ServiceFactory[Command, Response] =
    client.newClient(dest, label)

  def newService(dest: Name, label: String): Service[Command, Response] =
    client.newService(dest, label)

  object Server {
    /**
     * Default stack parameters used for memcached server.
     */
    val defaultParams: Stack.Params = StackServer.defaultParams +
      finagle.param.ProtocolLibrary("memcached")
  }

  case class Server(
      stack: Stack[ServiceFactory[Command, Response]] = StackServer.newStack,
      params: Stack.Params = Server.defaultParams)
    extends StdStackServer[Command, Response, Server] {

    protected def copy1(
      stack: Stack[ServiceFactory[Command, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = Response
    protected type Out = Command

    protected def newListener(): Listener[In, Out] = {
      Netty3Listener("memcached", MemcachedServerPipelineFactory)
    }

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[Command, Response]
    ): Closable = new SerialServerDispatcher(transport, service)

    // Java-friendly forwarders
    //See https://issues.scala-lang.org/browse/SI-8905
    override val withAdmissionControl: ServerAdmissionControlParams[Server] =
      new ServerAdmissionControlParams(this)
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

    override def configured[P](psp: (P, Stack.Param[P])): Server = super.configured(psp)
  }

  val server: Memcached.Server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[Command, Response]): ListeningServer =
    server.serve(addr, service)
}
