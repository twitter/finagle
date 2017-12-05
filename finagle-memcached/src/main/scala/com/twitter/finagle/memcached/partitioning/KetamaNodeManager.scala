package com.twitter.finagle.memcached.partitioning

import com.twitter.concurrent.Broker
import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.memcached._
import com.twitter.finagle.serverset2.addr.ZkMetadata
import com.twitter.finagle.service.FailedService
import com.twitter.hashing._
import com.twitter.util._
import java.net.InetSocketAddress
import scala.collection.{breakOut, mutable}

/**
 * Helper class for managing the nodes in the Ketama ring. Note that it tracks all addresses
 * as weighted addresses, which means a weight change for a given node will be considered a
 * node restart. This way implementations can adjust their partitions if weight is a factor
 * in partitioning.
 */
private[partitioning] class KetamaNodeManager[Req, Rep, Key](
  underlying: Stack[ServiceFactory[Req, Rep]],
  params: Stack.Params,
  numReps: Int = KetamaPartitioningService.DefaultNumReps
) { self =>

  private[this] val statsReceiver = {
    val param.Stats(stats) = params[param.Stats]
    stats.scope("partitioner")
  }
  private[this] val ejectionCount = statsReceiver.counter("ejections")
  private[this] val revivalCount = statsReceiver.counter("revivals")
  private[this] val nodeLeaveCount = statsReceiver.counter("leaves")
  private[this] val nodeJoinCount = statsReceiver.counter("joins")
  private[this] val keyRingRedistributeCount = statsReceiver.counter("redistributes")

  // nodes in the ketama ring, representing the backend services
  private[this] val nodes = mutable.Map[KetamaClientKey, Node]()

  private[this] val liveNodesGauge = statsReceiver.addGauge("live_nodes") {
    self.synchronized { nodes.count { case (_, Node(_, state)) => state == NodeState.Live } }
  }

  private[this] val deadNodesGauge = statsReceiver.addGauge("dead_nodes") {
    self.synchronized { nodes.count { case (_, Node(_, state)) => state == NodeState.Ejected } }
  }

  // used when all cache nodes are ejected from the cache ring
  private[this] val shardNotAvailableDistributor: Distributor[Future[Service[Req, Rep]]] =
    new SingletonDistributor(Future.value(new FailedService(new ShardNotAvailableException)))

  // We update those out of the request path so we need to make sure to synchronize on
  // read-modify-write operations on `currentDistributor` and `distributor`.
  // Note: Volatile-read from `partitionForKey` safety (not raciness) is guaranteed by JMM.
  @volatile private[this] var currentDistributor: Distributor[Future[Service[Req, Rep]]] =
    shardNotAvailableDistributor

  private[this] type KetamaKeyAndNode = (KetamaClientKey, KetamaNode[Future[Service[Req, Rep]]])

  // snapshot is used to detect new nodes when there is a change in bound addresses
  @volatile private[this] var snapshot: Set[KetamaKeyAndNode] = Set.empty

  // The nodeHealthBroker is use to track health of the nodes. Optionally, when the param
  // 'Memcached.param.EjectFailedHost' is true, unhealthy nodes are removed from the hash ring. It
  // connects the KetamaFailureAccrualFactory with the partition service to communicate the
  // health events.
  private[this] val nodeHealthBroker = new Broker[NodeHealth]

  // We also listen on a broker to eject/revive cache nodes.
  nodeHealthBroker.recv.foreach {
    case NodeMarkedDead(key) => ejectNode(key)
    case NodeRevived(key) => reviveNode(key)
  }

  private[this] sealed trait NodeState
  private[this] object NodeState {
    case object Live extends NodeState
    case object Ejected extends NodeState
  }

  // Node represents backend partition
  private[this] case class Node(
    node: KetamaNode[Future[Service[Req, Rep]]],
    var state: NodeState
  )

  private[this] val ketamaNodesChanges: Event[Set[KetamaKeyAndNode]] = {

    // Addresses in the current serverset that have been processed and have associated cache nodes.
    // Access synchronized on `self`
    var mapped: Map[Address, KetamaKeyAndNode] = Map.empty

    // Last set Addrs that have been processed.
    // Access synchronized on `self`
    var prevAddrs: Set[Address] = Set.empty

    // `map` is called on updates to `addrs`.
    // Cache nodes must only be created for new additions to the set of addresses; therefore
    // we must keep track of addresses in the current set that already have associated nodes
    val nodes: Var[Set[KetamaKeyAndNode]] = {
      // Intercept the params meant for Loadbalancer inserted by the BindingFactory
      val LoadBalancerFactory.Dest(dest: Var[Addr]) = params[LoadBalancerFactory.Dest]
      dest.map {
        case Addr.Bound(currAddrs, _) =>
          self.synchronized {
            // Add new nodes for new addresses by finding the difference between the two sets
            mapped ++= (currAddrs &~ prevAddrs).collect {
              case weightedAddr @ WeightedAddress(addr @ Address.Inet(ia, metadata), w) =>
                val (shardIdOpt: Option[String], boundAddress: Addr) =
                  metadata match {
                    case CacheNodeMetadata(_, shardId) =>
                      // This means the destination was resolved by TwitterCacheResolver.
                      twcacheConversion(shardId, ia)
                    case _ =>
                      ZkMetadata.fromAddrMetadata(metadata) match {
                        case Some(ZkMetadata(Some(shardId))) =>
                          (Some(shardId.toString), Addr.Bound(addr))
                        case _ =>
                          (None, Addr.Bound(addr))
                      }
                  }
                val node = CacheNode(ia.getHostName, ia.getPort, w.asInstanceOf[Int], shardIdOpt)
                val key = KetamaClientKey.fromCacheNode(node)
                val service = mkService(boundAddress, key)

                weightedAddr -> (
                  key -> KetamaNode[Future[Service[Req, Rep]]](
                    key.identifier,
                    node.weight,
                    service
                  )
                )
            }
            // Remove old nodes no longer in the serverset.
            mapped --= prevAddrs &~ currAddrs
            prevAddrs = currAddrs
          }
          mapped.values.toSet

        case _ =>
          Set.empty
      }
    }
    nodes.changes.filter(_.nonEmpty)
  }

  /**
   * This code is needed to support the old "twcache" scheme. The TwitterCacheResolver uses
   * CacheNodeMetadata instead of ZkMetadata for shardId. Also address is unresolved. Therefore
   * doing the necessary conversions here.
   */
  private[this] def twcacheConversion(
    shardId: Option[String],
    ia: InetSocketAddress
  ): (Option[String], Addr) = {
    val resolved = if (ia.isUnresolved) {
      new InetSocketAddress(ia.getHostName, ia.getPort)
    } else {
      ia
    }
    // Convert CacheNodeMetadata to ZkMetadata
    (
      shardId,
      Addr.Bound(
        Address.Inet(resolved, ZkMetadata.toAddrMetadata(ZkMetadata(shardId.map(_.toInt))))
      )
    )
  }

  // We listen for changes to the set of nodes to update the cache ring.
  private[this] val nodeWatcher: Closable = ketamaNodesChanges.respond(updateNodes)

  private[this] def mkService(addr: Addr, key: KetamaClientKey): Future[Service[Req, Rep]] = {
    val modifiedParams = params + LoadBalancerFactory.Dest(Var.value(addr))

    val next = underlying
      .replace(
        FailureAccrualFactory.role,
        KetamaFailureAccrualFactory.module[Req, Rep](key, nodeHealthBroker)
      )
      .make(modifiedParams)

    next().map { svc =>
      new ServiceProxy(svc) {
        override def close(deadline: Time): Future[Unit] = {
          Future.join(Seq(Closable.all(svc, next).close(deadline), super.close(deadline)))
        }
      }
    }
  }

  private[this] def rebuildDistributor(): Unit = self.synchronized {
    keyRingRedistributeCount.incr()

    val liveNodes = nodes.collect({ case (_, Node(node, NodeState.Live)) => node })(breakOut)

    currentDistributor = if (liveNodes.isEmpty) {
      shardNotAvailableDistributor
    } else {
      new KetamaDistributor(liveNodes, numReps, false /*oldLibMemcachedVersionComplianceMode*/ )
    }
  }

  private[this] def updateNodes(current: Set[KetamaKeyAndNode]): Unit = {
    self.synchronized {
      val old = snapshot
      // remove old nodes and release clients
      nodes --= (old &~ current).collect {
        case (key, node) =>
          node.handle.map { (service: Service[Req, Rep]) =>
            service.close()
          }
          nodeLeaveCount.incr()
          key
      }

      // new joined node appears as Live state
      nodes ++= (current &~ old).collect {
        case (key, node: KetamaNode[Future[Service[Req, Rep]]]) =>
          nodeJoinCount.incr()
          key -> Node(node, NodeState.Live)
      }

      snapshot = current
      rebuildDistributor()
    }
  }

  private[this] def ejectNode(key: KetamaClientKey) = self.synchronized {
    nodes.get(key) match {
      case Some(node) if node.state == NodeState.Live =>
        node.state = NodeState.Ejected
        rebuildDistributor()
        ejectionCount.incr()
      case _ =>
    }
  }

  private[this] def reviveNode(key: KetamaClientKey) = self.synchronized {
    nodes.get(key) match {
      case Some(node) if node.state == NodeState.Ejected =>
        node.state = NodeState.Live
        rebuildDistributor()
        revivalCount.incr()
      case _ =>
    }
  }

  def getServiceForHash(hash: Long): Future[Service[Req, Rep]] = {
    currentDistributor.nodeForHash(hash)
  }

  def close(deadline: Time): Future[Unit] = {
    nodeWatcher.close(deadline)
  }
}
