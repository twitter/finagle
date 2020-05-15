package com.twitter.finagle.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.partitioning.PartitionNodeManager.NoPartitionException
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle._
import com.twitter.util.{Await, Awaitable, Duration, Future, Time, Var}
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.atomic.AtomicBoolean
import org.scalatest.FunSuite
import scala.collection.immutable

class PartitionNodeManagerTest extends FunSuite {

  def await[T](a: Awaitable[T], d: Duration = 5.seconds): T =
    Await.result(a, d)

  def newAddress(inet: InetSocketAddress, weight: Int): Address = {
    val shardId = inet.getPort

    val md = ZkMetadata.toAddrMetadata(ZkMetadata(Some(shardId)))
    val addr = new Address.Inet(inet, md) {
      override def toString: String = s"Address(${inet.getPort})-($shardId)"
    }
    WeightedAddress(addr, weight)
  }

  class Ctx(addressSize: Int) {

    val stringService = new Service[String, String] {
      def apply(request: String): Future[String] = Future.value("service")

      val isOpen = new AtomicBoolean(true)
      override def status: Status = if (isOpen.get()) Status.Open else Status.Closed
      override def close(deadline: Time): Future[Unit] = {
        isOpen.set(false)
        Future.Done
      }
    }

    val inetAddresses =
      (0 until addressSize).map(_ => new InetSocketAddress(InetAddress.getLoopbackAddress, 0))

    // assign ports to the localhost addresses
    val fakeServers = inetAddresses.map { inet => StringServer.server.serve(inet, stringService) }
    val fixedInetAddresses = fakeServers.map(_.boundAddress.asInstanceOf[InetSocketAddress])
    val weightedAddress = fixedInetAddresses.map(newAddress(_, 1))

    val varAddr = Var(Addr.Bound(weightedAddress: _*))

    val factory: Stackable[ServiceFactory[String, String]] =
      new Stack.Module1[LoadBalancerFactory.Dest, ServiceFactory[String, String]] {
        val role = Stack.Role("serviceFactory")
        val description: String = "mock the Stack[ServiceFactory[Req, Rep] for node manager"

        def make(
          param: LoadBalancerFactory.Dest,
          next: ServiceFactory[String, String]
        ): ServiceFactory[String, String] = ServiceFactory.const(stringService)
      }

    val stack = new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
      .push(factory)
      .result

    val defaultParams = Stack.Params.empty + LoadBalancerFactory.Dest(varAddr)

    // p0(0), p1(1,2), p2(3,4,5), p3(6,...)
    // use port number to mock shardId
    def getLogicalPartition(varAddresses: Var[Seq[InetSocketAddress]]): Int => Int = { replica =>
      val addresses = varAddresses.sample()
      require(addresses.size >= 7)
      val partitionPositions = List(0.to(0), 1.to(2), 3.to(5), 6.until(addresses.size))
      val position = addresses.indexWhere(_.getPort == replica)
      val partitionId = partitionPositions.indexWhere(range => range.contains(position))
      scala.Predef.assert(partitionId > -1)
      partitionId
    }
  }

  test("Remove a partition, each node is a partition") {
    new Ctx(addressSize = 5) {
      val nodeManager = new PartitionNodeManager[String, String](
        stack,
        i => i,
        defaultParams
      )

      val svc0 = await(nodeManager.getServiceByPartitionId(fixedInetAddresses(0).getPort))

      varAddr.update(Addr.Bound(weightedAddress.drop(1): _*))

      intercept[NoPartitionException] {
        await(nodeManager.getServiceByPartitionId(fixedInetAddresses(0).getPort))
      }
      val svc1 = await(nodeManager.getServiceByPartitionId(fixedInetAddresses(1).getPort))
      assert(await(svc1("any")) == "service")
    }
  }

  test("Add a partition, each node is a partition") {
    new Ctx(addressSize = 5) {
      val nodeManager = new PartitionNodeManager[String, String](
        stack,
        i => i,
        defaultParams
      )

      val inet = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      // to get the port
      val newIsa =
        StringServer.server.serve(inet, stringService).boundAddress.asInstanceOf[InetSocketAddress]

      intercept[NoPartitionException] {
        await(nodeManager.getServiceByPartitionId(newIsa.getPort))
      }

      varAddr.update(Addr.Bound((weightedAddress :+ newAddress(newIsa, 1)): _*))

      await(nodeManager.getServiceByPartitionId(newIsa.getPort))
    }
  }

  test("replicas belong to the same logical partition") {
    new Ctx(addressSize = 7) {
      val logicalPartition = getLogicalPartition(Var(fixedInetAddresses))
      val nodeManager = new PartitionNodeManager[String, String](
        stack,
        logicalPartition,
        defaultParams
      )

      val svc00 = await(nodeManager.getServiceByPartitionId(0))
      val svc0 =
        await(nodeManager.getServiceByPartitionId(logicalPartition(fixedInetAddresses(0).getPort)))

      val svc10 = await(nodeManager.getServiceByPartitionId(1))
      val svc11 =
        await(nodeManager.getServiceByPartitionId(logicalPartition(fixedInetAddresses(1).getPort)))
      val svc12 =
        await(nodeManager.getServiceByPartitionId(logicalPartition(fixedInetAddresses(2).getPort)))
      assert(svc00 eq svc0)
      assert((svc10 eq svc11) && (svc10 eq svc12))
      assert(svc00 ne svc10)
    }
  }

  test("Add a node to an existing logical partition") {
    new Ctx(addressSize = 7) {
      val varInetAddress = Var(fixedInetAddresses)
      val logicalPartition = getLogicalPartition(varInetAddress)
      val nodeManager = new PartitionNodeManager[String, String](
        stack,
        logicalPartition,
        defaultParams
      )
      val inet = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      // to get the port
      val newIsa =
        StringServer.server.serve(inet, stringService).boundAddress.asInstanceOf[InetSocketAddress]

      // before adding, cannot find the logical partition
      intercept[AssertionError] {
        await(nodeManager.getServiceByPartitionId(logicalPartition(newIsa.getPort)))
      }

      val newAddresses = fixedInetAddresses :+ newIsa
      varInetAddress.update(newAddresses)
      varAddr.update(Addr.Bound(newAddresses.map(newAddress(_, 1)): _*))

      val svc4 = await(nodeManager.getServiceByPartitionId(3))
      val svc44 = await(nodeManager.getServiceByPartitionId(logicalPartition(newIsa.getPort)))
      assert(svc4 eq svc44)
    }
  }

  test("Remove a node from a partition has == 1 node") {
    new Ctx(addressSize = 7) {
      val logicalPartition = getLogicalPartition(Var(fixedInetAddresses))
      val nodeManager = new PartitionNodeManager[String, String](
        stack,
        logicalPartition,
        defaultParams
      )

      await(nodeManager.getServiceByPartitionId(0))

      // topology: p0(0), p1(1,2), p2(3,4,5), p3(6)
      // partition 0 has one address, drop it
      varAddr.update(Addr.Bound(weightedAddress.drop(1): _*))
      val e = intercept[NoPartitionException] {
        await(nodeManager.getServiceByPartitionId(0))
      }
      assert(e.getMessage.contains("No partition: 0 found in the node manager"))
    }
  }

  test("Remove a node from a partition has > 1 nodes") {
    new Ctx(addressSize = 8) {
      val nodeManager = new PartitionNodeManager[String, String](
        stack,
        getLogicalPartition(Var(fixedInetAddresses)),
        defaultParams
      )

      await(nodeManager.getServiceByPartitionId(0))

      // topology: p0(0), p1(1,2), p2(3,4,5), p3(6,7)
      // partition 3 has two address, remove one.
      varAddr.update(Addr.Bound(weightedAddress.dropRight(1): _*))

      await(nodeManager.getServiceByPartitionId(3))
      // remove both
      varAddr.update(Addr.Bound(weightedAddress.dropRight(2): _*))

      val e = intercept[NoPartitionException] {
        await(nodeManager.getServiceByPartitionId(3))
      }
      assert(e.getMessage.contains("No partition: 3 found in the node manager"))
    }
  }

  test("Node manager listens to weight changes") {
    new Ctx(addressSize = 8) {
      val varInetAddressHelper = Var(fixedInetAddresses)
      val nodeManager = new PartitionNodeManager[String, String](
        stack,
        getLogicalPartition(varInetAddressHelper),
        defaultParams
      )

      val newWeightedAddress =
        weightedAddress.dropRight(1) :+ newAddress(fixedInetAddresses.last, 2)

      // for testing propose, we want to see if weight changes can trigger the node manager
      // to rebuild the partition map. If rebuild, it will run into a failing getLogicalPartition
      // and log errors.
      varInetAddressHelper.update(immutable.IndexedSeq.empty)

      // we should log exceptions here
      varAddr.update(Addr.Bound(newWeightedAddress: _*))
    }
  }

  test("Addresses refresh, each node is a partition") {
    new Ctx(addressSize = 3) {
      val nodeManager = new PartitionNodeManager[String, String](
        stack,
        i => i,
        defaultParams
      )

      val svc0 = await(nodeManager.getServiceByPartitionId(fixedInetAddresses(0).getPort))

      // wipe out addresses won't trigger rebuild
      varAddr.update(Addr.Bound(Set.empty[Address]))
      val svc1 = await(nodeManager.getServiceByPartitionId(fixedInetAddresses(0).getPort))
      assert(svc0 eq svc1)

      // rebuild
      varAddr.update(Addr.Bound(fixedInetAddresses.map(newAddress(_, 2)): _*))
      val svc2 = await(nodeManager.getServiceByPartitionId(fixedInetAddresses(0).getPort))
      assert(svc0 ne svc2)

      // Neg won't trigger rebuild
      varAddr.update(Addr.Neg)
      val svc3 = await(nodeManager.getServiceByPartitionId(fixedInetAddresses(0).getPort))
      assert(svc2 eq svc3)

      // rebuild
      varAddr.update(Addr.Bound(fixedInetAddresses.map(newAddress(_, 3)): _*))
      val svc4 = await(nodeManager.getServiceByPartitionId(fixedInetAddresses(0).getPort))
      assert(svc4 ne svc0)
    }
  }

  test("close the node manager will close all ServiceFactories") {
    new Ctx(addressSize = 5) {
      val nodeManager = new PartitionNodeManager[String, String](
        stack,
        i => i,
        defaultParams
      )
      val svc0 = await(nodeManager.getServiceByPartitionId(fixedInetAddresses(0).getPort))
      assert(svc0.status == Status.Open)
      await(nodeManager.close())
      val svc1 = await(nodeManager.getServiceByPartitionId(fixedInetAddresses(1).getPort))
      assert(svc0.status == Status.Closed)
      assert(svc1.status == Status.Closed)
    }
  }

  test("log errors when getLogicalPartition throws exceptions for certain shards") {
    new Ctx(addressSize = 8) {
      def getLogicalPartition: Int => Int = {
        case even if even % 2 == 0 => 0
        case odd => throw new Exception("failed")
      }
      val nodeManager = new PartitionNodeManager[String, String](
        stack,
        getLogicalPartition,
        defaultParams
      )

      val succeedPort = fixedInetAddresses.map(_.getPort).filter(_ % 2 == 0)
      if (succeedPort.nonEmpty) {
        val svc0 = await(nodeManager.getServiceByPartitionId(0))
        assert(await(svc0("any")) == "service")
      }
    }
  }
}
