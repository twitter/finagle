package com.twitter.finagle.zookeeper

import com.google.common.collect.ImmutableSet
import com.twitter.common.net.pool.DynamicHostSet
import com.twitter.common.zookeeper.ServerSet
import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.group.StabilizingGroup
import com.twitter.finagle.{Group, Resolver, InetResolver, Addr}
import com.twitter.thrift.{Endpoint, ServiceInstance}
import com.twitter.thrift.Status.ALIVE
import com.twitter.util.{Future, Return, Throw, Try, Var}
import java.net.{InetSocketAddress, SocketAddress}
import scala.collection.JavaConverters._

class ZkResolverException(msg: String) extends Exception(msg)

private[finagle] class ZkGroup(serverSet: ServerSet, path: String)
    extends Thread("ZkGroup(%s)".format(path))
    with Group[ServiceInstance]
{
  setDaemon(true)
  start()

  protected[finagle] val set = Var(Set[ServiceInstance]())

  override def run() {
    serverSet.monitor(new DynamicHostSet.HostChangeMonitor[ServiceInstance] {
      def onChange(newSet: ImmutableSet[ServiceInstance]) = synchronized {
        set() = newSet.asScala.toSet
      }
    })
  }
}

class ZkResolver(factory: ZkClientFactory) extends Resolver {
  val scheme = "zk"

  def this() = this(DefaultZkClientFactory)

  def resolve(zkHosts: Set[InetSocketAddress], 
      path: String, endpoint: Option[String] = None, shardId: Option[Int] = None): Var[Addr] = {
    val (zkClient, zkHealthHandler) = factory.get(zkHosts)
    // prepare a filter appropriate to match ServiceInstances for the given Options
    val instanceFilter: ServiceInstance => Boolean = {
      val shardIdFilter: ServiceInstance => Boolean =
        shardId match {
          case Some(shardId) =>
            si => si.isSetShard && si.shard == shardId
          case None =>
            _ => true
        }
      val endpointFilter: ServiceInstance => Boolean =
        endpoint match {
          case Some(endpoint) =>
            si => si.getAdditionalEndpoints.containsKey(endpoint)
          case None =>
            _ => true
        }
      si => shardIdFilter(si) && endpointFilter(si)
    }
    // and an extractor for the appropriate endpoint on the ServiceInstance
    val endpointExtractor: ServiceInstance => Endpoint =
      endpoint match {
        case Some(endpoint) =>
          si => si.getAdditionalEndpoints.get(endpoint)
        case None =>
          si => si.getServiceEndpoint
      }

    // then use the filter and extractor to define a Group, and wrap it in a Var
    // TODO: get rid of Groups underneath.
    val zkGroup =
      (new ZkGroup(new ServerSetImpl(zkClient, path), path)) collect {
        case inst if inst.getStatus == ALIVE && instanceFilter(inst) =>
          val ep = endpointExtractor(inst)
          new InetSocketAddress(ep.getHost, ep.getPort): SocketAddress
      }
    val g = StabilizingGroup(
      zkGroup,
      zkHealthHandler,
      factory.sessionTimeout,
      DefaultStatsReceiver.scope("zkGroup"))

    g.set map { newSet => Addr.Bound(newSet) }
  }

  private[this] def zkHosts(hosts: String) = {
    val zkHosts = factory.hostSet(hosts)
    if (zkHosts.isEmpty)
      throw new ZkResolverException("ZK client address \"%s\" resolves to nothing".format(hosts))
    zkHosts
  }

  def bind(arg: String) = arg.split("!") match {
    // zk!host:2181!/path
    case Array(hosts, path) =>
      resolve(zkHosts(hosts), path, None)

    // zk!host:2181!/path!endpoint
    case Array(hosts, path, endpoint) =>
      resolve(zkHosts(hosts), path, Some(endpoint))

    case _ =>
      throw new ZkResolverException("Invalid address \"%s\"".format(arg))
  }
}
