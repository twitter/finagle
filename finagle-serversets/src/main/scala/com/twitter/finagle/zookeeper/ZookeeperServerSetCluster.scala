package com.twitter.finagle.zookeeper

import com.google.common.collect.ImmutableSet

import com.twitter.common.net.pool.DynamicHostSet
import com.twitter.common.zookeeper.ServerSet
import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.concurrent.Spool
import com.twitter.finagle.builder.Cluster
import com.twitter.thrift.ServiceInstance
import com.twitter.thrift.Status.ALIVE
import com.twitter.util.{Future, Return, Promise}

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet

/**
 * A Cluster of SocketAddresses that provide a certain service. Cluster
 * membership is indicated by children Zookeeper node.
 */
@deprecated("Get a Group[SocketAddress] from ZkResolver instead", "6.7.1")
class ZookeeperServerSetCluster(serverSet: ServerSet, endpointName: Option[String])
extends Cluster[SocketAddress] {

  def this(serverSet: ServerSet) = this(serverSet, None)
  def this(serverSet: ServerSet, endpointName: String) = this(serverSet, Some(endpointName))

  /**
   * LIFO "queue" of length one. Last-write-wins when more than one item
   * is enqueued.
   */
  private[this] val queuedChange = new AtomicReference[ImmutableSet[ServiceInstance]](null)
  // serverSet.monitor will block until initial membership is available
  private[zookeeper] val thread: Thread = new Thread("ServerSetMonitorInit") {
    override def run{
      serverSet.watch(new DynamicHostSet.HostChangeMonitor[ServiceInstance] {
        def onChange(serverSet: ImmutableSet[ServiceInstance]) = {
          val lastValue = queuedChange.getAndSet(serverSet)
          val firstToChange = lastValue eq null
          if (firstToChange) {
            var mostRecentValue: ImmutableSet[ServiceInstance] = null
            do {
              mostRecentValue = queuedChange.get
              performChange(mostRecentValue)
            } while (!queuedChange.compareAndSet(mostRecentValue, null))
          }
        }
      })
    }
  }
  thread.setDaemon(true)
  thread.start()

  private[this] val underlyingSet = new HashSet[SocketAddress]
  private[this] var changes = new Promise[Spool[Cluster.Change[SocketAddress]]]

  private[this] def performChange(serverSet: ImmutableSet[ServiceInstance]) = synchronized {
    val newSet = serverSet flatMap { serviceInstance =>
      val endpoint =
        endpointName match {
          case Some(name) => Option(serviceInstance.getAdditionalEndpoints.get(name))
          case None => Some(serviceInstance.getServiceEndpoint)
        }

      endpoint map { endpoint =>
        new InetSocketAddress(endpoint.getHost, endpoint.getPort): SocketAddress
      }
    }
    val added = newSet &~ underlyingSet
    val removed = underlyingSet &~ newSet
    added foreach { address =>
      underlyingSet += address
      appendUpdate(Cluster.Add(address))
    }
    removed foreach { address =>
      underlyingSet -= address
      appendUpdate(Cluster.Rem(address))
    }
  }

  private[this] def appendUpdate(update: Cluster.Change[SocketAddress]) = {
    val newTail = new Promise[Spool[Cluster.Change[SocketAddress]]]
    changes() = Return(update *:: newTail)
    changes = newTail
  }

  def joinServerSet(
    address: SocketAddress,
    endpoints: Map[String, InetSocketAddress] = Map[String, InetSocketAddress]()
  ): EndpointStatus = {
    require(address.isInstanceOf[InetSocketAddress])

    serverSet.join(
      address.asInstanceOf[InetSocketAddress],
      endpoints,
      ALIVE)
  }

  def join(
    address: SocketAddress,
    endpoints: Map[String, InetSocketAddress] = Map[String, InetSocketAddress]()
  ): Unit = joinServerSet(address, endpoints)

  def snap: (Seq[SocketAddress], Future[Spool[Cluster.Change[SocketAddress]]]) = synchronized {
    (underlyingSet.toSeq, changes)
  }
}
