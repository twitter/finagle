package com.twitter.finagle.partitioning

import com.twitter.concurrent.Broker
import com.twitter.finagle
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.partitioning.param.NumReps
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.service.FailedService
import com.twitter.finagle.{param => _, _}
import com.twitter.hashing._
import com.twitter.util._
import scala.collection.mutable

/**
 * Helper class for managing the nodes in the hash ring. Note that it tracks all addresses
 * as weighted addresses, which means a weight change for a given node will be considered a
 * node restart. This way implementations can adjust their partitions if weight is a factor
 * in partitioning.
 */
private[partitioning] class HashRingNodeManager[Req, Rep, Key](
  underlying: Stack[ServiceFactory[Req, Rep]],
  params: Stack.Params,
  numReps: Int = NumReps.Default) { self =>

  private[this] val statsReceiver = {
    val finagle.param.Stats(stats) = params[finagle.param.Stats]
    stats.scope("partitioner")
  }
  private[this] val ejectionCount = statsReceiver.counter("ejections")
  private[this] val revivalCount = statsReceiver.counter("revivals")
  private[this] val nodeLeaveCount = statsReceiver.counter("leaves")
  private[this] val nodeJoinCount = statsReceiver.counter("joins")
  private[this] val keyRingRedistributeCount = statsReceiver.counter("redistributes")

  // nodes in the hashing ring, representing the backend services
  private[this] val nodes = mutable.Map[HashNodeKey, Node]()

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

  private[this] type HashNodeKeyAndNode = (HashNodeKey, HashNode[Future[Service[Req, Rep]]])

  // snapshot is used to detect new nodes when there is a change in bound addresses
  @volatile private[this] var snapshot: Set[HashNodeKeyAndNode] = Set.empty

  // The nodeHealthBroker is use to track health of the nodes. Optionally, when the param
  // 'param.EjectFailedHost' is true, unhealthy nodes are removed from the hash ring. It
  // connects the ConsistentHashingFailureAccrualFactory with the partition service to communicate the
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
  private[this] case class Node(node: HashNode[Future[Service[Req, Rep]]], var state: NodeState)

  // constructs the appropriate HashNode entry
  private[this] def constructAddressEntry(
    node: PartitionNode,
    service: Future[Service[Req, Rep]]
  ): (HashNodeKey, HashNode[Future[Service[Req, Rep]]]) = {
    val key = HashNodeKey.fromPartitionNode(node)
    key -> HashNode[Future[Service[Req, Rep]]](
      key.identifier,
      node.weight,
      service
    )
  }

  // converts from the finagle `Address` format to the finagle-partitioning
  // HashNode entry format.
  private[this] val addressToEntry: PartialFunction[
    Address,
    (Address, (HashNodeKey, HashNode[Future[Service[Req, Rep]]]))
  ] = {
    case address =>
      address -> (address match {
        case WeightedAddress(addr @ Address.Inet(ia, metadata), w) =>
          val (shardIdOpt: Option[String], boundAddress: Addr) =
            ZkMetadata.fromAddrMetadata(metadata) match {
              case Some(ZkMetadata(Some(shardId), _)) =>
                (Some(shardId.toString), Addr.Bound(addr))
              case _ =>
                (None, Addr.Bound(addr))
            }
          val node = PartitionNode(ia.getHostName, ia.getPort, w.toInt, shardIdOpt)
          val key = HashNodeKey.fromPartitionNode(node)
          val service = mkService(boundAddress, key)
          constructAddressEntry(node, service)
        case WeightedAddress(Address.ServiceFactory(factory, metadata), w) =>
          val shardIdOpt = ZkMetadata.fromAddrMetadata(metadata).flatMap(_.shardId.map(_.toString))
          val node = PartitionNode(
            metadata("hostname").toString,
            metadata("port").asInstanceOf[Int],
            w.toInt,
            shardIdOpt
          )
          val service = factory().asInstanceOf[Future[Service[Req, Rep]]]
          constructAddressEntry(node, service)
      })
  }

  private[this] val hashRingNodesChanges: Event[Set[HashNodeKeyAndNode]] = {

    // Addresses in the current serverset that have been processed and have associated cache nodes.
    // Access synchronized on `self`
    var mapped: Map[Address, HashNodeKeyAndNode] = Map.empty

    // Last set Addrs that have been processed.
    // Access synchronized on `self`
    var prevAddrs: Set[Address] = Set.empty

    // `map` is called on updates to `addrs`.
    // Cache nodes must only be created for new additions to the set of addresses; therefore
    // we must keep track of addresses in the current set that already have associated nodes
    val nodes: Var[Set[HashNodeKeyAndNode]] = {
      // Intercept the params meant for Loadbalancer inserted by the BindingFactory
      val LoadBalancerFactory.Dest(dest: Var[Addr]) = params[LoadBalancerFactory.Dest]
      dest.map {
        case Addr.Bound(currAddrs, _) =>
          self.synchronized {
            // Add new nodes for new addresses by finding the difference between the two sets
            mapped ++= (currAddrs &~ prevAddrs).collect(addressToEntry)
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

  // We listen for changes to the set of nodes to update the cache ring.
  private[this] val nodeWatcher: Closable = hashRingNodesChanges.respond(updateNodes)

  private[this] def mkService(addr: Addr, key: HashNodeKey): Future[Service[Req, Rep]] = {
    val modifiedParams = params + LoadBalancerFactory.Dest(Var.value(addr))

    val next = underlying
      .replace(
        FailureAccrualFactory.role,
        ConsistentHashingFailureAccrualFactory.module[Req, Rep](key, nodeHealthBroker)
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

    val liveNodes = nodes.collect({ case (_, Node(node, NodeState.Live)) => node }).toSeq

    currentDistributor = if (liveNodes.isEmpty) {
      shardNotAvailableDistributor
    } else {
      new ConsistentHashingDistributor(
        liveNodes,
        numReps,
        false /*oldLibMemcachedVersionComplianceMode*/ )
    }
  }

  private[this] def updateNodes(current: Set[HashNodeKeyAndNode]): Unit = {
    self.synchronized {
      val old = snapshot
      // remove old nodes and release clients
      nodes --= (old &~ current).collect {
        case (key, node) =>
          node.handle.map { (service: Service[Req, Rep]) => service.close() }
          nodeLeaveCount.incr()
          key
      }

      // new joined node appears as Live state
      nodes ++= (current &~ old).collect {
        case (key, node: HashNode[Future[Service[Req, Rep]]]) =>
          nodeJoinCount.incr()
          key -> Node(node, NodeState.Live)
      }

      snapshot = current
      rebuildDistributor()
    }
  }

  private[this] def ejectNode(key: HashNodeKey) = self.synchronized {
    nodes.get(key) match {
      case Some(node) if node.state == NodeState.Live =>
        node.state = NodeState.Ejected
        rebuildDistributor()
        ejectionCount.incr()
      case _ =>
    }
  }

  private[this] def reviveNode(key: HashNodeKey) = self.synchronized {
    nodes.get(key) match {
      case Some(node) if node.state == NodeState.Ejected =>
        node.state = NodeState.Live
        rebuildDistributor()
        revivalCount.incr()
      case _ =>
    }
  }

  def getServiceForHash(hash: Long): Future[Service[Req, Rep]] =
    currentDistributor.nodeForHash(hash)

  def getPartitionIdForHash(hash: Long): Long =
    currentDistributor.partitionIdForHash(hash)

  def close(deadline: Time): Future[Unit] = {
    nodeWatcher.close(deadline)
  }
}
