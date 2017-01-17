package com.twitter.finagle

import _root_.java.net.SocketAddress
import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle.client.{ClientRegistry, DefaultPool, StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.{GenSerialClientDispatcher, PipeliningDispatcher, SerialServerDispatcher}
import com.twitter.finagle.loadbalancer.{Balancers, ConcurrentLoadBalancerFactory, LoadBalancerFactory}
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.Toggles
import com.twitter.finagle.memcached.exp.LocalMemcached
import com.twitter.finagle.memcached.protocol.text.CommandToEncoding
import com.twitter.finagle.memcached.protocol.text.client.ClientTransport
import com.twitter.finagle.memcached.protocol.text.server.ServerTransport
import com.twitter.finagle.memcached.protocol.text.client.DecodingToResponse
import com.twitter.finagle.memcached.protocol.text.transport.{Netty3ClientFramer, Netty3ServerFramer, Netty4ClientFramer, Netty4ServerFramer}
import com.twitter.finagle.memcached.protocol.{Command, Response, RetrievalCommand, Values}
import com.twitter.finagle.netty3.{Netty3Listener, Netty3Transporter}
import com.twitter.finagle.netty4.{Netty4Listener, Netty4Transporter}
import com.twitter.finagle.param.{ExceptionStatsHandler => _, Monitor => _, ResponseClassifier => _, Tracer => _, _}
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server.{Listener, StackServer, StdStackServer}
import com.twitter.finagle.service._
import com.twitter.finagle.service.exp.FailureAccrualPolicy
import com.twitter.finagle.stats.{ExceptionStatsHandler, StatsReceiver}
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.toggle.Toggle
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.hashing
import com.twitter.io.Buf
import com.twitter.util.{Closable, Duration, Monitor}
import com.twitter.util.registry.GlobalRegistry
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

    /**
     * Configure the [[Transporter]] and [[Listener]] implementation
     * used by Memcached.
     */
    case class MemcachedImpl(
        transporter: Stack.Params => Transporter[Buf, Buf],
        listener: Stack.Params => Listener[Buf, Buf]) {
      def mk(): (MemcachedImpl, Stack.Param[MemcachedImpl]) =
        (this, MemcachedImpl.param)
    }

    object MemcachedImpl {
      /**
       * A [[MemcachedImpl]] that uses netty3 as the underlying I/O multiplexer.
       */
      val Netty3 = MemcachedImpl(
        params => Netty3Transporter[Buf, Buf](Netty3ClientFramer, params),
        params => Netty3Listener[Buf, Buf](Netty3ServerFramer, params))

      /**
       * A [[MemcachedImpl]] that uses netty4 as the underlying I/O multiplexer.
       *
       * @note Important! This is experimental and not yet tested in production!
       */
      val Netty4 = MemcachedImpl(
        params => Netty4Transporter[Buf, Buf](Netty4ClientFramer, params),
        params => Netty4Listener[Buf, Buf](Netty4ServerFramer, params))

      private[this] val UseNetty4ToggleId: String =
        "com.twitter.finagle.memcached.UseNetty4"

      private[this] val netty4Toggle: Toggle[Int] = Toggles(UseNetty4ToggleId)
      private[this] def useNetty4: Boolean = netty4Toggle(ServerInfo().id.hashCode)

      implicit val param: Stack.Param[MemcachedImpl] = Stack.Param(
        if (useNetty4) Netty4
        else Netty3
      )
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
    val defaultParams: Stack.Params = StackClient.defaultParams +
      FailureAccrualFactory.Param(defaultFailureAccrualPolicy) +
      FailFastFactory.FailFast(false) +
      LoadBalancerFactory.Param(Balancers.p2cPeakEwma()) +
      PendingRequestFilter.Param(limit = defaultPendingRequestLimit) +
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

    protected type In = Buf
    protected type Out = Buf

    protected def newTransporter(): Transporter[In, Out] =
      params[param.MemcachedImpl].transporter(params)

    protected def newDispatcher(transport: Transport[In, Out]): Service[Command, Response] =
      new PipeliningDispatcher(
        new ClientTransport[Command, Response](
          new CommandToEncoding,
          new DecodingToResponse,
          transport),
        params[finagle.param.Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope),
        DefaultTimer.twitter
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

      registerClient(label0, hasher.toString, isPipelining = true)

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

    // Java-friendly forwarders
    // See https://issues.scala-lang.org/browse/SI-8905
    override val withLoadBalancer: ConcurrentLoadBalancingParams[Client] =
      new ConcurrentLoadBalancingParams(this)
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

  /**
   * A Memcached server that should be used only for testing
   */
  case class Server(
      stack: Stack[ServiceFactory[Command, Response]] = StackServer.newStack,
      params: Stack.Params = Server.defaultParams)
    extends StdStackServer[Command, Response, Server] {

    protected def copy1(
      stack: Stack[ServiceFactory[Command, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = Buf
    protected type Out = Buf

    protected def newListener(): Listener[In, Out] =
      params[param.MemcachedImpl].listener(params)

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

    override def configured[P](psp: (P, Stack.Param[P])): Server = super.configured(psp)
  }

  val server: Memcached.Server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[Command, Response]): ListeningServer =
    server.serve(addr, service)
}
