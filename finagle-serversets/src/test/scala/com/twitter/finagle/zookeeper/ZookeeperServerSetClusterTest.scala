package com.twitter.finagle.zookeeper

import com.google.common.collect.ImmutableSet
import com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor
import com.twitter.common.zookeeper.ServerSet
import com.twitter.conversions.time._
import com.twitter.finagle.builder.Cluster
import com.twitter.thrift.{Endpoint, ServiceInstance, Status}
import com.twitter.util.Await
import java.net.{InetSocketAddress, SocketAddress}
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.anyObject
import org.mockito.Mockito.{when, verify}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ZookeeperServerSetClusterSpec extends FunSuite with MockitoSugar {
  val port1 = 80 // not bound
  val port2 = 53 // ditto

  type EndpointMap = Map[String, InetSocketAddress]
  val EmptyEndpointMap = Map.empty[String, InetSocketAddress]

  def forClient(
    endpointName: Option[String]
  )(
    f: (ZookeeperServerSetCluster, (InetSocketAddress, EndpointMap) => Unit) => Unit
  ) {
    val serverSet = mock[ServerSet]
    val monitorCaptor = ArgumentCaptor.forClass(classOf[HostChangeMonitor[ServiceInstance]])

    val cluster = new ZookeeperServerSetCluster(serverSet, endpointName)
    cluster.thread.join()

    verify(serverSet).watch(monitorCaptor.capture)
    val clusterMonitor = monitorCaptor.getValue()

    def registerHost(socketAddr: InetSocketAddress, extraEndpoints: EndpointMap) {
      val additionalEndpoints = extraEndpoints map { case (name, addr) =>
        name -> new Endpoint(addr.getHostName, addr.getPort)
      }

      val serviceInstance = new ServiceInstance(
        new Endpoint(socketAddr.getHostName, socketAddr.getPort),
        additionalEndpoints.asJava,
        Status.ALIVE
      )
      val serviceInstances = ImmutableSet.builder[ServiceInstance].add(serviceInstance).build()

      clusterMonitor.onChange(serviceInstances)
    }

    f(cluster, registerHost)
  }

  test("ZookeeperServerSetCluster registers the server with ZooKeeper") {
    val serverSet = mock[ServerSet]
    when(
      serverSet.join(anyObject, anyObject, anyObject[Status])
    ).thenReturn(mock[ServerSet.EndpointStatus])

    val cluster = new ZookeeperServerSetCluster(serverSet)
    cluster.thread.join()

    val localAddress = new InetSocketAddress(port1)
    cluster.join(localAddress)

    verify(serverSet).join(localAddress, EmptyEndpointMap.asJava, Status.ALIVE)
  }

  test("ZookeeperServerSetCluster registers the server with multiple endpoints") {
    val serverSet = mock[ServerSet]
    when(
      serverSet.join(anyObject, anyObject, anyObject[Status])
    ).thenReturn(mock[ServerSet.EndpointStatus])

    val cluster = new ZookeeperServerSetCluster(serverSet)
    cluster.thread.join()

    val localAddress = new InetSocketAddress(port1)
    val altLocalAddress = new InetSocketAddress(port2)

    cluster.join(localAddress, Map("alt" -> altLocalAddress))

    verify(serverSet).join(localAddress, Map("alt" -> altLocalAddress).asJava, Status.ALIVE)
  }

  // CSL-2175
  if (!sys.props.contains("SKIP_FLAKY")) {
    test("ZookeeperServerSetCluster receives a registration from ZooKeeper") {
      forClient(None) { (cluster, registerHost) =>
        val (current, futureChanges) = cluster.snap
        assert(current.isEmpty)

        val remoteAddress = new InetSocketAddress("host", port1)
        registerHost(remoteAddress, EmptyEndpointMap)

        val changes = Await.result(futureChanges, 1.minute)
        assert(changes.head == Cluster.Add(remoteAddress: SocketAddress))
      }
    }

    test("ZookeeperServerSetCluster is able to block till server set is ready") {
      forClient(None) { (cluster, registerHost) =>
        assert(!cluster.ready.isDefined)

        val remoteAddress = new InetSocketAddress("host", port1)
        registerHost(remoteAddress, EmptyEndpointMap)

        assert(cluster.ready.isDefined)
        val (hosts, _) = cluster.snap
        assert(hosts == Seq(remoteAddress: SocketAddress))
      }
    }

    test("ZookeeperServerSetCluster is able to use an additional endpoint") {
      forClient(Some("other-endpoint")) { (cluster, registerHost) =>
        val (current, futureChanges) = cluster.snap
        assert(current.isEmpty)

        val remoteAddress = new InetSocketAddress("host", port1)
        val otherRemoteAddress = new InetSocketAddress("host", port2)
        registerHost(remoteAddress, Map("other-endpoint" -> otherRemoteAddress))

        val changes = Await.result(futureChanges, 1.minute)
        assert(changes.head == Cluster.Add(otherRemoteAddress: SocketAddress))
      }
    }

    test("ZookeeperServerSetCluster ignores a server which does not specify the additional endpoint") {
      forClient(Some("this-endpoint")) { (cluster, registerHost) =>
        val remoteAddress = new InetSocketAddress("host", port1)
        registerHost(remoteAddress, EmptyEndpointMap)
        assert(!cluster.ready.isDefined)
      }
    }
  }
}
