package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.delivery.thriftscala.DeliveryService._
import com.twitter.delivery.thriftscala._
import com.twitter.finagle.Addr
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.param.CommonParams
import com.twitter.finagle.partitioning.ConsistentHashPartitioningService.NoPartitioningKeys
import com.twitter.finagle.partitioning.PartitionNodeManager.NoPartitionException
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy._
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.finagle.thrift.{ThriftRichClient, ThriftRichServer}
import com.twitter.finagle.{Address, ListeningServer, Name, Stack}
import com.twitter.scrooge.ThriftStructIface
import com.twitter.util.{Activity, Await, Awaitable, Duration, Future, Return, Throw, Var}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

abstract class PartitionAwareClientEndToEndTest extends AnyFunSuite {

  def await[T](a: Awaitable[T], d: Duration = 5.seconds): T =
    Await.result(a, d)

  type ClientType <: Stack.Parameterized[ClientType] with WithThriftPartitioningStrategy[
    ClientType
  ] with ThriftRichClient

  type ServerType <: Stack.Parameterized[ServerType] with CommonParams[
    ServerType
  ] with ThriftRichServer

  def clientImpl(): ClientType

  def serverImpl(): ServerType

  def newAddress(inet: InetSocketAddress, weight: Int): Address = {
    val shardId = inet.getPort

    val md = ZkMetadata.toAddrMetadata(ZkMetadata(Some(shardId)))
    val addr = new Address.Inet(inet, md) {
      override def toString: String = s"Address(${inet.getPort})-($shardId)"
    }
    WeightedAddress(addr, weight)
  }

  trait Ctx {

    val iface = new DeliveryService.MethodPerEndpoint {
      def getBox(addrInfo: AddrInfo, passcode: Byte): Future[Box] =
        Future.value(Box(addrInfo, "hi"))

      def getBoxes(
        addrs: collection.Seq[AddrInfo],
        passcode: Byte
      ): Future[collection.Seq[Box]] =
        Future.value(
          addrs.map(Box(_, s"size: ${addrs.size}")) // shows how many sub-requests hit an endpoint
        )

      def sendBox(box: Box): Future[String] = Future.value(box.item)

      def sendBoxes(boxes: collection.Seq[Box]): Future[String] =
        Future.value(boxes.map(_.item).mkString(","))
    }

    // response merger functions
    val getBoxesRepMerger: ResponseMerger[Seq[Box]] = (successes, failures) =>
      if (successes.nonEmpty) Return(successes.flatten)
      else Throw(failures.head)

    val sendBoxesRepMerger: ResponseMerger[String] = (successes, failures) =>
      if (successes.nonEmpty) Return(successes.mkString(";"))
      else Throw(failures.head)

    val inetAddresses: Seq[InetSocketAddress] =
      (0 until 5).map(_ => new InetSocketAddress(InetAddress.getLoopbackAddress, 0))

    val servers: Seq[ListeningServer] =
      inetAddresses.map(inet => serverImpl().serveIface(inet, iface))

    def fixedInetAddresses = servers.map(_.boundAddress.asInstanceOf[InetSocketAddress])

    def addresses = servers.map { server =>
      val inet = server.boundAddress.asInstanceOf[InetSocketAddress]
      newAddress(inet, 1)
    }

    val addrInfo0 = AddrInfo("zero", 12345)
    val addrInfo1 = AddrInfo("one", 11111)
    val addrInfo11 = AddrInfo("one", 11112)
    val addrInfo2 = AddrInfo("two", 22222)
    val addrInfo3 = AddrInfo("three", 33333)
    val addrInfo4 = AddrInfo("four", 44444)
  }

  trait HashingPartitioningCtx extends Ctx {
    // request merger functions -- only used for hashing case when multiple keys fall in the same shard
    val getBoxesReqMerger: RequestMerger[GetBoxes.Args] = listGetBoxes =>
      GetBoxes.Args(listGetBoxes.map(_.listAddrInfo).flatten, listGetBoxes.head.passcode)
    val sendBoxesReqMerger: RequestMerger[SendBoxes.Args] = listSendBoxes =>
      SendBoxes.Args(listSendBoxes.flatMap(_.boxes))

    val hashingPartitioningStrategy = new ClientHashingStrategy({
      case getBox: GetBox.Args => Map(getBox.addrInfo.name -> getBox)
      case getBoxes: GetBoxes.Args =>
        getBoxes.listAddrInfo
          .groupBy {
            _.name
          }.map {
            case (hashingKey, subListAddrInfo) =>
              hashingKey -> GetBoxes.Args(subListAddrInfo, getBoxes.passcode)
          }
      case sendBoxes: SendBoxes.Args =>
        sendBoxes.boxes.groupBy(_.addrInfo.name).map {
          case (hashingKey, boxes) => hashingKey -> SendBoxes.Args(boxes)
        }
    })

    hashingPartitioningStrategy.requestMergerRegistry
      .add(GetBoxes, getBoxesReqMerger)
      .add(SendBoxes, sendBoxesReqMerger)

    hashingPartitioningStrategy.responseMergerRegistry
      .add(GetBoxes, getBoxesRepMerger)
      .add(SendBoxes, sendBoxesRepMerger)
  }

  test("without partition strategy") {
    new Ctx {
      val client = clientImpl()
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      val expectedOneNode =
        Seq(Box(addrInfo1, "size: 3"), Box(addrInfo2, "size: 3"), Box(addrInfo3, "size: 3"))

      val result = await(client.getBoxes(Seq(addrInfo1, addrInfo2, addrInfo3), Byte.MinValue))
      assert(result == expectedOneNode)
      assert(await(client.sendBox(Box(addrInfo1, "test"))) == "test")
      assert(
        await(
          client.sendBoxes(Seq(Box(addrInfo1, "test1"), Box(addrInfo2, "test2")))) == "test1,test2")
      assert(await(client.getBox(addrInfo1, Byte.MinValue)) == Box(addrInfo1, "hi"))
      client.asClosable.close()
      servers.map(_.close())
    }
  }

  test("with consistent hashing strategy") {
    new HashingPartitioningCtx {
      val client = clientImpl().withPartitioning
        .strategy(hashingPartitioningStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      // addrInfo1 and addrInfo11 have the same key ("one")
      // addrInfo2 key ("two")
      val expectedTwoNodes =
        Seq(Box(addrInfo1, "size: 2"), Box(addrInfo11, "size: 2"), Box(addrInfo2, "size: 1")).toSet
      val expectedOneNode =
        Seq(Box(addrInfo1, "size: 3"), Box(addrInfo11, "size: 3"), Box(addrInfo2, "size: 3")).toSet

      val result =
        await(client.getBoxes(Seq(addrInfo1, addrInfo2, addrInfo11), Byte.MinValue)).toSet
      // if two keys hash to a singleton partition, expect one node, otherwise, two nodes.
      // local servers have random ports that are not consistent
      assert(result == expectedOneNode || result == expectedTwoNodes)

      client.asClosable.close()
      servers.map(_.close())
    }
  }

  test("with consistent hashing strategy, unspecified endpoint returns error") {
    new HashingPartitioningCtx {
      val client = clientImpl().withPartitioning
        .strategy(hashingPartitioningStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      val e = intercept[NoPartitioningKeys] {
        await(client.sendBox(Box(addrInfo1, "test")))
      }
      assert(e.getMessage.contains("sendBox"))
      client.asClosable.close()
      servers.map(_.close())
    }
  }

  test("with errored hashing strategy") {
    val erroredHashingPartitioningStrategy = new ClientHashingStrategy({
      case getBox: GetBox.Args => throw new Exception("something wrong")
    })

    new HashingPartitioningCtx {
      val client = clientImpl().withPartitioning
        .strategy(erroredHashingPartitioningStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      intercept[PartitioningStrategyException] {
        await(client.getBox(addrInfo1, Byte.MinValue))
      }

      client.asClosable.close()
      servers.map(_.close())
    }
  }

  test("with custom partitioning strategy, each shard is a partition") {
    new Ctx {
      // structs with key zero/two are routed to partition 0,
      // and structs with key one are routed to partition 1
      def lookUp(addrInfo: AddrInfo): Int = {
        addrInfo.name match {
          case "zero" | "two" => fixedInetAddresses(0).getPort
          case "one" => fixedInetAddresses(1).getPort
        }
      }

      val customPartitioningStrategy = ClientCustomStrategy.noResharding({
        case getBox: GetBox.Args => Future.value(Map(lookUp(getBox.addrInfo) -> getBox))
        case getBoxes: GetBoxes.Args =>
          val partitionIdAndRequest: Map[Int, ThriftStructIface] =
            getBoxes.listAddrInfo.groupBy(lookUp).map {
              case (partitionId, listAddrInfo) =>
                partitionId -> GetBoxes.Args(listAddrInfo, getBoxes.passcode)
            }
          Future.value(partitionIdAndRequest)
      })

      customPartitioningStrategy.responseMergerRegistry.add(GetBoxes, getBoxesRepMerger)

      val client = clientImpl().withPartitioning
        .strategy(customPartitioningStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      val expectedTwoNodes =
        Seq(Box(addrInfo0, "size: 2"), Box(addrInfo2, "size: 2"), Box(addrInfo1, "size: 1")).toSet

      val result = await(client.getBoxes(Seq(addrInfo0, addrInfo1, addrInfo2), Byte.MinValue)).toSet
      assert(result == expectedTwoNodes)
    }
  }

  class CustomPartitioningCtx(lookUp: AddrInfo => Int) extends Ctx {
    val customPartitioningStrategy = ClientCustomStrategy.noResharding(
      {
        case getBox: GetBox.Args => Future.value(Map(lookUp(getBox.addrInfo) -> getBox))
        case getBoxes: GetBoxes.Args =>
          val partitionIdAndRequest: Map[Int, ThriftStructIface] =
            getBoxes.listAddrInfo.groupBy(lookUp).map {
              case (partitionId, listAddrInfo) =>
                partitionId -> GetBoxes.Args(listAddrInfo, getBoxes.passcode)
            }
          Future.value(partitionIdAndRequest)
      },
      { instance: Int => // p0(0,1), p1(1,2), p2(3, 4)
        val partitionPositions = List(0.to(1), 1.to(2), 3.until(fixedInetAddresses.size))
        val position = fixedInetAddresses.indexWhere(_.getPort == instance)
        partitionPositions.zipWithIndex
          .filter { case (range, _) => range.contains(position) }.map(_._2)
      }
    )

    customPartitioningStrategy.responseMergerRegistry.add(GetBoxes, getBoxesRepMerger)
  }

  test("with custom partitioning strategy, logical partition") {
    // struct keys to partition Ids
    def lookUp(addrInfo: AddrInfo): Int = {
      addrInfo.name match {
        case "four" => 0
        case "three" | "two" => 1
        case "one" | "zero" => 2
      }
    }

    new CustomPartitioningCtx(lookUp) {
      val client = clientImpl().withPartitioning
        .strategy(customPartitioningStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      val expectedThreeNodes =
        Seq(
          Box(addrInfo0, "size: 2"),
          Box(addrInfo1, "size: 2"),
          Box(addrInfo2, "size: 2"),
          Box(addrInfo3, "size: 2"),
          Box(addrInfo4, "size: 1")).toSet

      val result = await(
        client.getBoxes(
          Seq(addrInfo0, addrInfo1, addrInfo2, addrInfo3, addrInfo4),
          Byte.MinValue)).toSet
      assert(result == expectedThreeNodes)
    }
  }

  test("with errored custom strategy") {
    val erroredCustomPartitioningStrategy = ClientCustomStrategy.noResharding({
      case getBox: GetBox.Args => throw new Exception("something wrong")
    })

    new Ctx {
      val client = clientImpl().withPartitioning
        .strategy(erroredCustomPartitioningStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      intercept[PartitioningStrategyException] {
        await(client.getBox(addrInfo0, Byte.MinValue))
      }

      client.asClosable.close()
      servers.map(_.close())
    }
  }

  test("with custom strategy, no logical partition") {
    def lookUp(addrInfo: AddrInfo): Int = {
      addrInfo.name match {
        case _ => 4
      }
    }

    new CustomPartitioningCtx(lookUp) {
      val client = clientImpl().withPartitioning
        .strategy(customPartitioningStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      intercept[NoPartitionException] {
        await(
          client
            .getBoxes(Seq(addrInfo0, addrInfo1, addrInfo2, addrInfo3, addrInfo4), Byte.MinValue))
      }
    }
  }

  test("with custom strategy, unset endpoint") {
    // struct keys to partition Ids
    def lookUp(addrInfo: AddrInfo): Int = {
      addrInfo.name match {
        case "four" => 0
        case "three" | "two" => 1
        case "one" | "zero" => 2
      }
    }

    new CustomPartitioningCtx(lookUp) {
      val client = clientImpl().withPartitioning
        .strategy(customPartitioningStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      intercept[PartitioningStrategyException] {
        await(client.sendBoxes(Seq(Box(addrInfo1, "test1"), Box(addrInfo2, "test2"))))
      }
    }
  }

  test("with custom strategy, partitioning strategy dynamically changing") {
    new Ctx {
      val sr0 = new InMemoryStatsReceiver
      val sr1 = new InMemoryStatsReceiver
      var index = 0

      // for testing purpose, set separate statsReceivers for instance0 and instance1
      override val servers: Seq[ListeningServer] =
        inetAddresses.map { inet =>
          if (index == 0) {
            index = index + 1
            serverImpl().withStatsReceiver(sr0).serveIface(inet, iface)
          } else if (index == 1) {
            index = index + 1
            serverImpl().withStatsReceiver(sr1).serveIface(inet, iface)
          } else {
            serverImpl().serveIface(inet, iface)
          }
        }

      // the observable state is a dynamic Integer,
      // when state changed, we route requests to a partition with current Integer as the Id
      val dynamic = Var(0)
      val observable: Activity[Int] = Activity(dynamic.map(Activity.Ok(_)))
      val getPartitionIdAndRequest: Int => ClientCustomStrategy.ToPartitionedMap = { state =>
        {
          case sendBox: SendBox.Args =>
            Future.value(Map(state -> sendBox))
        }
      }
      val getLogicalPartitionId: Int => Int => Seq[Int] = {
        _ =>
          { instance: Int =>
            Seq(fixedInetAddresses.indexWhere(_.getPort == instance))
          }
      }

      val dynamicStrategy = ClientCustomStrategy.resharding[Int](
        getPartitionIdAndRequest,
        getLogicalPartitionId,
        observable
      )

      val client = clientImpl().withPartitioning
        .strategy(dynamicStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      await(client.sendBox(Box(addrInfo0, "")))
      dynamic() += 1
      await(client.sendBox(Box(addrInfo0, "")))
      val server0Request =
        if (sr0.counters.isDefinedAt(Seq("requests"))) sr0.counters(Seq("requests"))
        else sr0.counters(Seq("thrift", "requests"))

      val server1Request =
        if (sr1.counters.isDefinedAt(Seq("requests"))) sr1.counters(Seq("requests"))
        else sr1.counters(Seq("thrift", "requests"))

      assert(server0Request == 1)
      assert(server1Request == 1)
    }
  }

  test("with cluster resharding, expanding cluster's instances") {
    new Ctx {

      val sr = new InMemoryStatsReceiver
      var index = 0

      // set instance 1 a stats receiver
      override val servers: Seq[ListeningServer] =
        inetAddresses.map { inet =>
          if (index == 0) {
            index = index + 1
            serverImpl().serveIface(inet, iface)
          } else if (index == 1) {
            index = index + 1
            serverImpl().withStatsReceiver(sr).serveIface(inet, iface)
          } else {
            serverImpl().serveIface(inet, iface)
          }
        }

      val getPartitionIdAndRequest: Set[Address] => ClientCustomStrategy.ToPartitionedMap = { _ =>
        {
          case sendBox: SendBox.Args =>
            Future.value(Map(1 -> sendBox)) // always route requests to partition 1
        }
      }

      val getLogicalPartitionId: Set[Address] => Int => Seq[Int] = {
        cluster =>
          { instance: Int =>
            if (cluster.size == 2) { // p0(0) p1(1)
              Seq(fixedInetAddresses.indexWhere(_.getPort == instance))
            } else { //p0(0,1) p1(2,3,4)
              val partitionPositions = List(0.to(1), 2.until(fixedInetAddresses.size))
              val position = fixedInetAddresses.indexWhere(_.getPort == instance)
              partitionPositions.zipWithIndex
                .filter { case (range, _) => range.contains(position) }.map(_._2)
            }
          }
      }
      val clusterResharding =
        ClientCustomStrategy.clusterResharding(getPartitionIdAndRequest, getLogicalPartitionId)

      val dynamicAddresses = Var(Addr.Bound(addresses.take(2): _*))
      val client = clientImpl().withPartitioning
        .strategy(clusterResharding)
        .build[DeliveryService.MethodPerEndpoint](
          Name.Bound(dynamicAddresses, dynamicAddresses()),
          "client")

      // pre-resharding, the request goes to server1,
      // post-resharding, the request goes to server2/3/4
      await(client.sendBox(Box(addrInfo0, "")))
      val server1Request =
        if (sr.counters.isDefinedAt(Seq("requests"))) sr.counters(Seq("requests"))
        else sr.counters(Seq("thrift", "requests"))

      assert(server1Request == 1)
      dynamicAddresses() = Addr.Bound(addresses: _*)
      await(client.sendBox(Box(addrInfo0, "")))
      assert(server1Request == 1)

    }
  }
}
