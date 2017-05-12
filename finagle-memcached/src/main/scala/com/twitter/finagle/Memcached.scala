package com.twitter.finagle

import _root_.java.net.SocketAddress
import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle.client.{ClientRegistry, DefaultPool, StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.{GenSerialClientDispatcher, PipeliningDispatcher, SerialServerDispatcher}
import com.twitter.finagle.loadbalancer.{Balancers, LoadBalancerFactory}
import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureAccrualPolicy}
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.exp.LocalMemcached
import com.twitter.finagle.memcached.loadbalancer.ConcurrentLoadBalancerFactory
import com.twitter.finagle.memcached.protocol.text.client.ClientTransport
import com.twitter.finagle.memcached.protocol.text.client.DecodingToResponse
import com.twitter.finagle.memcached.protocol.text.CommandToEncoding
import com.twitter.finagle.memcached.protocol.text.server.ServerTransport
import com.twitter.finagle.memcached.protocol.text.transport.{Netty4ClientFramer, Netty4ServerFramer}
import com.twitter.finagle.memcached.protocol.{Command, Response, RetrievalCommand, Values}
import com.twitter.finagle.netty4.{Netty4HashedWheelTimer, Netty4Listener, Netty4Transporter}
import com.twitter.finagle.param.{ExceptionStatsHandler => _, Monitor => _, ResponseClassifier => _, Tracer => _, _}
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server.{Listener, StackServer, StdStackServer}
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.hashing
import com.twitter.io.Buf
import com.twitter.util.registry.GlobalRegistry
import com.twitter.util.{Closable, Duration, Monitor}
import scala.collection.mutable

private[finagle] object MemcachedTracingFilter {

  object Module extends Stack.Module1[param.Label, ServiceFactory[Command, Response]] {
    val role = ClientTracingFilter.role
    val description = "Add Memcached client specific annotations to the trace"

    def make(_label: param.Label, next: ServiceFactory[Command, Response]) = {
      val param.Label(label) = _label
      val annotations = new AnnotatingTracingFilter[Command, Response](
        label, Annotation.ClientSend(), Annotation.ClientRecv())
      annotations andThen TracingFilter andThen next
    }
  }

  object TracingFilter extends SimpleFilter[Command, Response] {
    def apply(command: Command, service: Service[Command, Response]) = {
      val response = service(command)
      if (Trace.isActivelyTracing) {
        // Submitting rpc name here assumes there is no further tracing lower in the stack
        Trace.recordRpc(command.name)
        command match {
          case command: RetrievalCommand =>
            response.onSuccess {
              case Values(vals) =>
                val cmd = command.asInstanceOf[RetrievalCommand]
                val misses = mutable.Set.empty[String]
                cmd.keys.foreach { case Buf.Utf8(key) => misses += key }
                vals.foreach { value =>
                  val Buf.Utf8(key) = value.key
                  Trace.recordBinary(key, "Hit")
                  misses.remove(key)
                }
                misses.foreach {
                  Trace.recordBinary(_, "Miss")
                }
              case _ =>
            }
          case _ =>
            response
        }
      } else {
        response
      }
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

    /**
     * We use 10 consecutive errors as we have experience that per connection rps within a client
     * is a lot lower than the overall rps generated by a service and memcached backends do not
     * tend to show transient errors. Clients should backoff early and long enough to let a failing
     * backend removed from the cache ring.
     * per_conn_rps = total_rps / (number_of_clients * number_of_backends * number_of_conn_per_client)
     */
    private[this] val defaultFailureAccrualPolicy = () => FailureAccrualPolicy.consecutiveFailures(
      10, Backoff.const(30.seconds))

    /**
     * To prevent an ever growing pipeline which can cause increased memory pressure on the client,
     * we set a limit to the number of pending requests allowed. Requests overflowing this limit
     * will return a RejectedExecutionException.
     */
    private[this] val defaultPendingRequestLimit = Some(100)

    /**
     * Default stack parameters used for memcached client. We change the
     * load balancer to `p2cPeakEwma` as we have experience improved tail
     * latencies when coupled with the pipelining dispatcher.
     */
    private val params: Stack.Params = StackClient.defaultParams +
      FailureAccrualFactory.Param(defaultFailureAccrualPolicy) +
      FailFastFactory.FailFast(false) +
      LoadBalancerFactory.Param(Balancers.p2cPeakEwma()) +
      PendingRequestFilter.Param(limit = defaultPendingRequestLimit) +
      ProtocolLibrary(ProtocolLibraryName) +
      Timer(Netty4HashedWheelTimer)

    /**
     * A default client stack which supports the pipelined memcached client.
     * The `ConcurrentLoadBalancerFactory` load balances over a small set of
     * duplicate endpoints to eliminate head of line blocking. Each endpoint
     * has a single pipelined connection.
     */
    private val stack: Stack[ServiceFactory[Command, Response]] = StackClient.newStack
      .replace(LoadBalancerFactory.role, ConcurrentLoadBalancerFactory.module[Command, Response])
      .replace(DefaultPool.Role, SingletonPool.module[Command, Response])
      .replace(ClientTracingFilter.role, MemcachedTracingFilter.Module)

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
    hasher: String
  ): Unit = {
    GlobalRegistry.get.put(
      Seq(ClientRegistry.registryName, Client.ProtocolLibraryName, label, "key_hasher"),
      hasher)
  }

  /**
   * A memcached client with support for pipelined requests, consistent hashing,
   * and per-node load-balancing.
   */
  case class Client(
      stack: Stack[ServiceFactory[Command, Response]] = Client.stack,
      params: Stack.Params = Client.params)
    extends StdStackClient[Command, Response, Client]
    with MemcachedRichClient {

    import Client.mkDestination

    protected def copy1(
      stack: Stack[ServiceFactory[Command, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf

    protected def newTransporter(addr: SocketAddress): Transporter[In, Out] =
      Netty4Transporter.raw(Netty4ClientFramer, addr, params)

    protected def newDispatcher(transport: Transport[In, Out]): Service[Command, Response] =
      new PipeliningDispatcher(
        new ClientTransport[Command, Response](
          new CommandToEncoding,
          new DecodingToResponse,
          transport),
        params[finagle.param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope),
        DefaultTimer
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

      val label0 = if (label == "") params[Label].label else label

      registerClient(label0, hasher.toString)

      val healthBroker = new Broker[NodeHealth]

      def newService(node: CacheNode): Service[Command, Response] = {
        val key = KetamaClientKey.fromCacheNode(node)
        val stk = stack.replace(FailureAccrualFactory.role,
          KetamaFailureAccrualFactory.module[Command, Response](key, healthBroker))
        withStack(stk).newService(mkDestination(node.host, node.port), label0)
      }

      val scopedSr = sr.scope(label0)
      new KetamaPartitionedClient(va, newService, healthBroker, scopedSr, hasher, numReps)
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

    /**
     * Configures the number of concurrent `connections` a single endpoint has.
     * The connections are load balanced over which allows the pipelined client to
     * avoid head-of-line blocking and reduce its latency.
     *
     * We've empirically found that four is a good default for this, but it can be
     * increased at the cost of additional connection overhead.
     */
    def connectionsPerEndpoint(connections: Int): Client =
      configured(ConcurrentLoadBalancerFactory.Param(connections))

    override val withTransport: ClientTransportParams[Client] =
      new ClientTransportParams(this)
    override val withSession: ClientSessionParams[Client] =
      new ClientSessionParams(this)
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

    override def withStack(stack: Stack[ServiceFactory[Command, Response]]): Client =
      super.withStack(stack)
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
    private val params: Stack.Params = StackServer.defaultParams +
      ProtocolLibrary("memcached") +
      Timer(Netty4HashedWheelTimer)
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

    protected def newListener(): Listener[In, Out] =
      Netty4Listener[Buf, Buf](Netty4ServerFramer, params)

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[Command, Response]
    ): Closable = new SerialServerDispatcher(new ServerTransport(transport), service)

    // Java-friendly forwarders
    //See https://issues.scala-lang.org/browse/SI-8905
    override val withAdmissionControl: ServerAdmissionControlParams[Server] =
      new ServerAdmissionControlParams(this)
    override val withSession: SessionParams[Server] =
      new SessionParams(this)
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
    override def configured[P](psp: (P, Stack.Param[P])): Server = super.configured(psp)
  }

  val server: Memcached.Server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[Command, Response]): ListeningServer =
    server.serve(addr, service)
}
