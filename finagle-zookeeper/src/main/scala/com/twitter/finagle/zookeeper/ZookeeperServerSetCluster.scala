package com.twitter.finagle.zookeeper

import java.net.{InetSocketAddress, SocketAddress}
import com.twitter.common.zookeeper.ServerSet
import collection.SeqProxy
import java.util.concurrent.atomic.AtomicReference

//val zookeeperClient = new ZooKeeperClient(
//-          Amount.of(100, Time.MILLISECONDS),
//-          _zookeeperHosts.get)
//-        val serverSet = new ServerSetImpl(zookeeperClient, host.path)
//-        new ServerSetBrokerPool(serverSet, wrapBrokerWithStats(codec, _))

class ZookeeperServerSetCluster(serverSet: ServerSet) extends Cluster {
  def mkBrokers[B <: Broker](f: SocketAddress => B) = new SeqProxy {
    @volatile private[this] var underlyingMap = Map[SocketAddress, B]()
    def self = underlyingMap.values.toSeq

    // Note: this is a LIFO "queue" of length one. Last-write-wins.
    @volatile private[this] val queuedChange =
      new AtomicReference[ImmutableSet[ServiceInstance]](null)

    serverSet.monitor(new DynamicHostSet.HostChangeMonitor[ServiceInstance] {
      def onChange(serverSet: ImmutableSet[ServiceInstance]) = {
        val lastValue = queuedChange.getAndSet(serverSet)
        val firstToChange = lastValue eq null
        if (firstToChange) {
          do {
            performChange(serverSet)
          } while (!queuedChange.compareAndSet(serverSet, null))
        }
      }
    })

    private[this] def performChange(serverSet: ServerSet) {
      val newSet = serverSet flatMap { serviceInstance =>
        val endpoint = serviceInstance.getServiceEndpoint
        val address = new InetSocketAddress(endpoint.getHost, endpoint.getPort)

        if (serviceInstance.getStatus == Status.ALIVE) Some(address)
        else None
      }
      val oldMap = underlyingMap
      val oldSet = oldMap.keys.toSet
      val removed = oldSet &~ newSet
      val same = oldSet & newSet
      val added = newSet -- oldMap.keys

      val newMap = Map(added.toSeq map { address =>
        address -> mkBroker(address)
      }: _*) ++ oldMap.filter { case (key, value) => same contains key }
      underlyingMap = newMap
      removed.foreach { address =>
        oldMap(address).close()
      }
    }
  }
}