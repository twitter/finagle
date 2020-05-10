package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.delivery.thriftscala.DeliveryService._
import com.twitter.delivery.thriftscala._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.partitioning.ConsistentHashPartitioningService.{
  HashingStrategyException,
  NoPartitioningKeys
}
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy._
import com.twitter.finagle.thrift.{ThriftRichClient, ThriftRichServer}
import com.twitter.finagle.{Address, ListeningServer, Name, Stack}
import com.twitter.util.{Await, Awaitable, Duration, Future, Return, Throw}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.FunSuite

abstract class PartitionAwareClientEndToEndTest extends FunSuite {

  def await[T](a: Awaitable[T], d: Duration = 5.seconds): T =
    Await.result(a, d)

  type ClientType <: Stack.Parameterized[ClientType] with WithThriftPartitioningStrategy[
    ClientType
  ] with ThriftRichClient

  def clientImpl(): ClientType

  def serverImpl(): ThriftRichServer

  trait Ctx {
    val addrInfo1 = AddrInfo("one", 12345)
    val addrInfo2 = AddrInfo("two", 54321)
    val addrInfo3 = AddrInfo("one", 98765)

    val iface = new DeliveryService.MethodPerEndpoint {
      def getBox(addrInfo: AddrInfo, passcode: Byte): Future[Box] =
        Future.value(Box(addrInfo, "hi"))

      def getBoxes(
        addrInfo: collection.Seq[AddrInfo],
        passcode: Byte
      ): Future[collection.Seq[Box]] =
        Future.value(addrInfo.map(Box(_, s"size: ${addrInfo.size}")))

      def sendBox(box: Box): Future[String] = Future.value(box.item)

      def sendBoxes(boxes: collection.Seq[Box]): Future[String] =
        Future.value(boxes.map(_.item).mkString(","))
    }

    // request merger functions -- only used for hashing case when multiple keys fall in the same shard
    val getBoxesReqMerger: RequestMerger[GetBoxes.Args] = listGetBoxes =>
      GetBoxes.Args(listGetBoxes.map(_.listAddrInfo).flatten, listGetBoxes.head.passcode)
    val sendBoxesReqMerger: RequestMerger[SendBoxes.Args] = listSendBoxes =>
      SendBoxes.Args(listSendBoxes.flatMap(_.boxes))

    // response merger functions
    val getBoxesRepMerger: ResponseMerger[Seq[Box]] = (successes, failures) =>
      if (successes.nonEmpty) Return(successes.flatten)
      else Throw(failures.head)

    val sendBoxesRepMerger: ResponseMerger[String] = (successes, failures) =>
      if (successes.nonEmpty) Return(successes.mkString(";"))
      else Throw(failures.head)

    val servers: Seq[ListeningServer] = {
      val inetAddresses = (1 to 5)
        .map(_ => new InetSocketAddress(InetAddress.getLoopbackAddress, 0))

      inetAddresses.map(inet => serverImpl().serveIface(inet, iface))
    }
  }

  trait PartitioningCtx extends Ctx {
    val hashingPartitioningStrategy = new ClientHashingStrategy {
      val getHashingKeyAndRequest: ToPartitionedMap = {
        case getBox: GetBox.Args => Map(getBox.addrInfo.name -> getBox)
        case getBoxes: GetBoxes.Args =>
          getBoxes.listAddrInfo
            .groupBy { _.name }.map {
              case (hashingKey, subListAddrInfo) =>
                hashingKey -> GetBoxes.Args(subListAddrInfo, getBoxes.passcode)
            }
        case sendBoxes: SendBoxes.Args =>
          sendBoxes.boxes.groupBy(_.addrInfo.name).map {
            case (hashingKey, boxes) => hashingKey -> SendBoxes.Args(boxes)
          }
      }

      override val requestMergerRegistry: RequestMergerRegistry =
        RequestMergerRegistry.create
          .add(GetBoxes, getBoxesReqMerger)
          .add(SendBoxes, sendBoxesReqMerger)

      override val responseMergerRegistry: ResponseMergerRegistry =
        ResponseMergerRegistry.create
          .add(GetBoxes, getBoxesRepMerger)
          .add(SendBoxes, sendBoxesRepMerger)
    }

    val addresses = servers.map { server =>
      val inet = server.boundAddress.asInstanceOf[InetSocketAddress]
      newAddress(inet, 1)
    }
  }

  test("without partition strategy") {
    new Ctx {
      val addresses = servers.map { server =>
        val inet = server.boundAddress.asInstanceOf[InetSocketAddress]
        newAddress(inet, 1)
      }

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
    new PartitioningCtx {
      val client = clientImpl().withPartitioning
        .strategy(hashingPartitioningStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      // addrInfo1 and addrInfo3 have the same key ("one")
      // addrInfo2 key ("two")
      val expectedTwoNodes =
        Seq(Box(addrInfo1, "size: 2"), Box(addrInfo3, "size: 2"), Box(addrInfo2, "size: 1")).toSet
      val expectedOneNode =
        Seq(Box(addrInfo1, "size: 3"), Box(addrInfo3, "size: 3"), Box(addrInfo2, "size: 3")).toSet

      val result =
        await(client.getBoxes(Seq(addrInfo1, addrInfo2, addrInfo3), Byte.MinValue)).toSet
      // if two keys hash to a singleton partition, expect one node, otherwise, two nodes.
      assert(result == expectedOneNode || result == expectedTwoNodes)

      client.asClosable.close()
      servers.map(_.close())
    }
  }

  test("with consistent hashing strategy, unspecified endpoint returns error") {
    new PartitioningCtx {
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
    val erroredHashingPartitioningStrategy = new ClientHashingStrategy {
      val getHashingKeyAndRequest: ToPartitionedMap = {
        case getBox: GetBox.Args => throw new Exception("something wrong")
      }
    }

    new PartitioningCtx {
      val client = clientImpl().withPartitioning
        .strategy(erroredHashingPartitioningStrategy)
        .build[DeliveryService.MethodPerEndpoint](Name.bound(addresses: _*), "client")

      intercept[HashingStrategyException] {
        await(client.getBox(addrInfo1, Byte.MinValue))
      }

      client.asClosable.close()
      servers.map(_.close())
    }
  }

  def newAddress(inet: InetSocketAddress, weight: Int): Address = {
    val shardId = inet.getPort

    val md = ZkMetadata.toAddrMetadata(ZkMetadata(Some(shardId)))
    val addr = new Address.Inet(inet, md) {
      override def toString: String = s"Address(${inet.getPort})-($shardId)"
    }
    WeightedAddress(addr, weight)
  }
}
