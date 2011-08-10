package com.twitter.finagle.zookeeper

import java.net.{InetSocketAddress, SocketAddress}
import com.twitter.common.zookeeper.ServerSet
import collection.SeqProxy
import java.util.concurrent.atomic.AtomicReference
import com.twitter.finagle.builder.Cluster
import com.google.common.collect.ImmutableSet
import com.twitter.common.net.pool.DynamicHostSet
import scala.collection.JavaConversions._
import com.twitter.thrift.ServiceInstance
import com.twitter.thrift.Status.ALIVE
import com.twitter.finagle.ServiceFactory

/**
 * A Cluster of SocketAddresses that provide a certain service. Cluster
 * membership is indicated by children Zookeeper node.
 */
class ZookeeperServerSetCluster(serverSet: ServerSet) extends Cluster {
  private[zookeeper] var thread: Thread = null

  def join(address: SocketAddress) {
    require(address.isInstanceOf[InetSocketAddress])

    serverSet.join(
      address.asInstanceOf[InetSocketAddress],
      Map[String, InetSocketAddress](),
      ALIVE)
  }

  def mkFactories[Req, Rep](mkBroker: (SocketAddress) => ServiceFactory[Req, Rep]) = {
    new SeqProxy[ServiceFactory[Req, Rep]] {
      @volatile private[this] var underlyingMap =
        Map[SocketAddress, ServiceFactory[Req, Rep]]()
      def self = underlyingMap.values.toSeq

      /**
       * LIFO "queue" of length one. Last-write-wins when more than one item
       * is enqueued.
       */
      private[this] val queuedChange =
        new AtomicReference[ImmutableSet[ServiceInstance]](null)

      // serverSet.monitor will block until initial membership is available
      thread = new Thread {
        override def run {
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
        }
      }
      thread.start()

      private[this] def performChange(serverSet: ImmutableSet[ServiceInstance]) {
        val oldMap = underlyingMap
        val newSet = collectIsAlive(serverSet.toSet)
        val (removed, same, added) = diff(oldMap.keys.toSet, newSet)

        val addedBrokers = Map(added.toSeq map { address =>
          address -> mkBroker(address)
        }: _*)
        val sameBrokers = oldMap.filter { case (key, value) => same contains key }
        val newMap = addedBrokers ++ sameBrokers
        underlyingMap = newMap
        removed.foreach { address =>
          oldMap(address).close()
        }
      }

      private[this] def diff[A](oldSet: Set[A], newSet: Set[A]) = {
        val removed = oldSet &~ newSet
        val same = oldSet & newSet
        val added = newSet &~ oldSet

        (removed, same, added)
      }

      private[this] def collectIsAlive(serverSet: Set[ServiceInstance]) = {
        val alive = serverSet.filter(_.getStatus == ALIVE)
        val addresses = serverSet.map { serviceInstance =>
          val endpoint = serviceInstance.getServiceEndpoint
          new InetSocketAddress(endpoint.getHost, endpoint.getPort): SocketAddress
        }
        addresses
      }
    }

  }
}
