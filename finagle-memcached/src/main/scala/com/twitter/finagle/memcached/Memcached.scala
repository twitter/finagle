package com.twitter.finagle.memcached

import _root_.java.net.InetSocketAddress

import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle._
import com.twitter.finagle.cacheresolver.{CacheNode, CacheNodeGroup}
import com.twitter.finagle.client.{DefaultPool, StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.service._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.hashing.{KetamaNode, KeyHasher}

object param {
  /**
   * Whether to eject cache host from the Ketama ring based on failure accrual.
   * By default, this is off. When turning on, keep the following caveat in
   * mind: ejection is based on local failure accrual, so your cluster may
   * get different views of the same cache host. With cache updates, this can
   * introduce inconsistency in cache data. In many cases, it's better to eject
   * cache host from a separate mechanism that's based on a global view.
   */
  case class EjectFailedHost(v: Boolean) {
    def mk(): (EjectFailedHost, Stack.Param[EjectFailedHost]) =
      (this, EjectFailedHost.param)
  }

  object EjectFailedHost {
    implicit val param = Stack.Param(EjectFailedHost(false))
  }
}

object Memcached {
  val defaultParams: Stack.Params =
    StackClient.defaultParams +
      FailureAccrualFactory.Param(100, () => 1.second) +
      FailFastFactory.FailFast(false) +
      ProtocolLibrary("memcached")
}

/**
 * Stack based Ketama Memcache builder. It builds a Ketama Memcache
 * client that dispatches through pipelining on a singleton pool,
 * and uses `KetamaFailureAccrualFactory` that fails immediately
 * when a node is marked dead.
 *
 * For example, a client can be built through:
 * {{{
 *   val client = Memcached(label).newClient(dest)
 * }}}
 * Note: newClient takes `Name` as param, and ONLY Bounded names are supported.
 *
 * If you want to provide finely tuned configurations:
 * {{{
 *   val client =
 *     Memcached(label)
 *       .configured(FailureAccrualFactory.Param(30, () => 1.seconds))
 *       // tcp connection timeout
 *       .configured(Transporter.ConnectTimeout(100.milliseconds))
 *       // one single request timeout
 *       .configured(TimeoutFilter.Param(requestTimeout))
 *       // the acquisition timeout of a connection
 *       .configured(TimeoutFactory.Param(serviceTimeout))
 *       .configured(EjectFailedHost(false))
 *       .newClient(dest)
 * }}}
 */
case class Memcached(
    label: String,
    keyHasher: KeyHasher = KeyHasher.KETAMA,
    params: Stack.Params = Memcached.defaultParams) {
  type MkStack =
    (KetamaClientKey, Broker[NodeHealth]) =>
      Stack[ServiceFactory[Command, Response]]

  type MkClient =
    (KetamaClientKey, Broker[NodeHealth]) => finagle.Client[Command, Response]

  private[this] val clientParams = params + finagle.param.Label(label)

  /**
   * Config Stack params for Memcache Client
   */
  def configured[P: Stack.Param](p: P): Memcached =
    copy(params = params + p)

  private[this] val finagle.param.Stats(stats) = params[finagle.param.Stats]

  object Client {
    private[this] def failureAccrualModule(
      key: KetamaClientKey,
      healthBroker: Broker[NodeHealth]
    ): Stackable[ServiceFactory[Command, Response]] = {
      new Stack.Module4[
          finagle.param.Stats,
          FailureAccrualFactory.Param,
          param.EjectFailedHost,
          finagle.param.Timer,
          ServiceFactory[Command, Response]]
      {
        val role = Stack.Role("KetamaFailureAccrual")
        val description =
          "Backoff from hosts that we cannot successfully make requests to"
        def make(
          _stats: finagle.param.Stats,
          _param: FailureAccrualFactory.Param,
          _eject: param.EjectFailedHost,
          _timer: finagle.param.Timer,
          next: ServiceFactory[Command, Response]
        ): ServiceFactory[Command, Response] = {
          val FailureAccrualFactory.Param(numFailures, markDeadFor) = _param
          val finagle.param.Timer(timer) = _timer
          val finagle.param.Stats(statsReceiver) = _stats
          val param.EjectFailedHost(eject) = _eject

          val wrapper = new ServiceFactoryWrapper {
            def andThen[Command, Response](
              factory: ServiceFactory[Command, Response]
            ) =
              new KetamaFailureAccrualFactory(
                factory, numFailures, markDeadFor, timer, key, healthBroker,
                eject, statsReceiver.scope("failure_accrual"))
          }
          wrapper.andThen(next)
        }
      }
    }

    def newStack(
      key: KetamaClientKey,
      broker: Broker[NodeHealth]
    ): Stack[ServiceFactory[Command, Response]] =
      StackClient.newStack
        .replace(FailureAccrualFactory.role, failureAccrualModule(key, broker))
        .replace(DefaultPool.Role, SingletonPool.module[Command, Response])
  }

  case class Client(
      key: KetamaClientKey,
      nodeHealthBroker: Broker[NodeHealth],
      params: Stack.Params = clientParams,
      mkStack: MkStack = Client.newStack)
    extends StdStackClient[Command, Response, Client] {
    protected type In = Command
    protected type Out = Response

    override def stack: Stack[ServiceFactory[Command, Response]] =
      mkStack(key, nodeHealthBroker)

    protected def copy1(
      stack: Stack[ServiceFactory[Command, Response]] = stack,
      params: Stack.Params = this.params
    ): Client = copy(params = params)

    override def newTransporter(): Transporter[Command, Response] = {
      val finagle.param.Label(label) = params[finagle.param.Label]
      val codec = (new text.Memcached(stats)).client(ClientCodecConfig(label))
      Netty3Transporter(codec.pipelineFactory, params)
    }

    override def newDispatcher(
      transport: Transport[Command, Response]
    ): Service[Command, Response] =
      new PipeliningDispatcher(transport)
  }

  private[this] val newClient: (KetamaClientKey, Broker[NodeHealth]) => Client =
    (key, broker) => Client(key, broker)

  private def mkGrp(
    // deprecate Group, use Var instead. CSL-1583
    initialServices: Group[CacheNode],
    nodeHealthBroker: Broker[NodeHealth],
    label: String,
    mkClient: MkClient
  ): Group[(KetamaClientKey, KetamaNode[com.twitter.finagle.memcached.Client])] =
    initialServices.map { node =>
      val key = node.key match {
        case Some(id) => KetamaClientKey(id)
        case None     => KetamaClientKey(node.host, node.port, node.weight)
      }
      val dest = Name.bound(new InetSocketAddress(node.host, node.port))
      val fClient: Service[Command, Response] =
        mkClient(key, nodeHealthBroker).newClient(dest, label).toService
      key -> KetamaNode(key.identifier, node.weight, TwemcacheClient(fClient))
    }

  private[this] class MemcachedClient(
      label: String,
      initialServices: Group[CacheNode],
      mkClient: MkClient,
      statsReceiver: StatsReceiver,
      nodeHealthBroker: Broker[NodeHealth] = new Broker[NodeHealth])
    extends KetamaPartitionedClient(
      ketamaNodeGrp    = mkGrp(initialServices, nodeHealthBroker, label, mkClient),
      nodeHealthBroker = nodeHealthBroker,
      statsReceiver    = statsReceiver,
      keyHasher        = keyHasher,
      numReps          = KetamaClient.DefaultNumReps,
      oldLibMemcachedVersionComplianceMode = false)

  def newClient(
    name: Name,
    mkClient: MkClient = newClient
  ): memcached.Client = {
    val va = name match {
      // memcache only support Bounded names. TRFC-162
      case Name.Bound(va) => va
      case _ => throw new Exception(
        "Memcache client only support Bounded Name, tracked in TRFC-162")
    }

    new MemcachedClient(
      label,
      CacheNodeGroup(Group.fromVarAddr(va)),
      mkClient,
      stats)
  }
}
