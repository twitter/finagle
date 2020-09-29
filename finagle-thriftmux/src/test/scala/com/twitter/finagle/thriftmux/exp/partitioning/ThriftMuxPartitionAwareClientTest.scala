package com.twitter.finagle.thriftmux.exp.partitioning

import com.twitter.delivery.thriftscala.DeliveryService.{GetBox, GetBoxes}
import com.twitter.delivery.thriftscala.{AddrInfo, Box, DeliveryService}
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.RequestMerger
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.finagle.thrift.exp.partitioning.{
  MethodBuilderCustomStrategy,
  MethodBuilderHashingStrategy,
  PartitionAwareClientEndToEndTest
}
import com.twitter.finagle.{Name, ThriftMux}
import com.twitter.util.Future

class ThriftMuxPartitionAwareClientTest extends PartitionAwareClientEndToEndTest {
  type ClientType = ThriftMux.Client
  type ServerType = ThriftMux.Server

  def clientImpl(): ThriftMux.Client = ThriftMux.client

  def serverImpl(): ThriftMux.Server = ThriftMux.server

  class MethodBuilderCtx extends Ctx {
    val builder = clientImpl().methodBuilder(Name.bound(addresses: _*))
    // request merger function
    val getBoxesReqMerger: RequestMerger[GetBoxes.Args] = listGetBoxes =>
      GetBoxes.Args(listGetBoxes.map(_.listAddrInfo).flatten, listGetBoxes.head.passcode)

    val mbHashingStrategy1 = new MethodBuilderHashingStrategy[GetBoxes.Args, Seq[Box]](
      { getBoxes: GetBoxes.Args =>
        getBoxes.listAddrInfo
          .groupBy { _.name }.map {
            case (hashingKey, subListAddrInfo) =>
              hashingKey -> GetBoxes.Args(subListAddrInfo, getBoxes.passcode)
          }
      },
      Some(getBoxesReqMerger),
      Some(getBoxesRepMerger)
    )

    val mbHashingStrategy2 = new MethodBuilderHashingStrategy[GetBox.Args, Box](
      { getBox: GetBox.Args =>
        Map(getBox.addrInfo.name -> getBox)
      }
    )

    def lookUp(addrInfo: AddrInfo): Int = {
      addrInfo.name match {
        case "four" => 0
        case "three" | "two" => 1
        case "one" | "zero" => 2
      }
    }

    // p0(0), p1(1,2), p2(3, 4)
    val getLogicalPartition: Int => Seq[Int] = { instance: Int =>
      val partitionPositions = List(0.until(1), 1.until(3), 3.until(fixedInetAddresses.size))
      val position = fixedInetAddresses.indexWhere(_.getPort == instance)
      Seq(partitionPositions.indexWhere(range => range.contains(position)))
    }

    val mbCustomStrategy1 = new MethodBuilderCustomStrategy[GetBoxes.Args, Seq[Box]](
      { getBoxes: GetBoxes.Args =>
        val partitionIdAndRequest: Map[Int, GetBoxes.Args] =
          getBoxes.listAddrInfo.groupBy(lookUp).map {
            case (partitionId, listAddrInfo) =>
              partitionId -> GetBoxes.Args(listAddrInfo, getBoxes.passcode)
          }
        Future.value(partitionIdAndRequest)

      },
      getLogicalPartition,
      Some(getBoxesRepMerger)
    )

    val mbCustomStrategy2 = new MethodBuilderCustomStrategy[GetBox.Args, Box](
      { getBox: GetBox.Args =>
        Future.value(Map(lookUp(getBox.addrInfo) -> getBox))
      },
      getLogicalPartition
    )
  }

  test("MethodBuilder with hashing strategy, each endpoint has its strategy") {
    new MethodBuilderCtx {
      val getBoxesEndpoint =
        builder
          .withPartitioningStrategy(mbHashingStrategy1)
          .servicePerEndpoint[DeliveryService.ServicePerEndpoint](methodName = "getBoxes")
          .getBoxes

      val getBoxEndpoint =
        builder
          .withPartitioningStrategy(mbHashingStrategy2)
          .servicePerEndpoint[DeliveryService.ServicePerEndpoint](methodName = "getBox")
          .getBox

      // addrInfo1 and addrInfo11 have the same key ("one")
      // addrInfo2 key ("two")
      val expectedTwoNodes =
        Seq(Box(addrInfo1, "size: 2"), Box(addrInfo11, "size: 2"), Box(addrInfo2, "size: 1")).toSet
      val expectedOneNode =
        Seq(Box(addrInfo1, "size: 3"), Box(addrInfo11, "size: 3"), Box(addrInfo2, "size: 3")).toSet

      val getBoxesResult =
        await(
          getBoxesEndpoint(
            GetBoxes.Args(Seq(addrInfo1, addrInfo2, addrInfo11), Byte.MinValue))).toSet

      val getBoxResult = await(getBoxEndpoint(GetBox.Args(addrInfo0, Byte.MinValue)))

      // if two keys hash to a singleton partition, expect one node, otherwise, two nodes.
      // local servers have random ports that are not consistent
      assert(getBoxesResult == expectedTwoNodes || getBoxesResult == expectedOneNode)
      assert(getBoxResult == Box(addrInfo0, "hi"))

      getBoxesEndpoint.close()
      getBoxEndpoint.close()
      servers.map(_.close())
    }
  }

  test("MethodBuilder with errored hashing strategy") {
    new MethodBuilderCtx {
      val erroredHashingPartitioningStrategy = new MethodBuilderHashingStrategy[GetBox.Args, Box](
        { _: GetBox.Args =>
          throw new Exception("something wrong")
        }
      )

      val endpoint = builder
        .withPartitioningStrategy(erroredHashingPartitioningStrategy)
        .servicePerEndpoint[DeliveryService.ServicePerEndpoint](methodName = "getBoxes")
        .getBoxes

      intercept[PartitioningStrategyException] {
        await(endpoint(GetBoxes.Args(Seq(addrInfo1, addrInfo2, addrInfo11), Byte.MinValue)))
      }

      endpoint.close()
      servers.map(_.close())
    }
  }

  test("MethodBuilder with custom strategy, each endpoint has its strategy") {
    new MethodBuilderCtx {
      val getBoxesEndpoint = builder
        .withPartitioningStrategy(mbCustomStrategy1)
        .servicePerEndpoint[DeliveryService.ServicePerEndpoint](methodName = "getBoxes")
        .getBoxes

      val getBoxEndpoint = builder
        .withPartitioningStrategy(mbCustomStrategy2)
        .servicePerEndpoint[DeliveryService.ServicePerEndpoint](methodName = "getBox")
        .getBox

      val expectedThreeNodes =
        Seq(
          Box(addrInfo0, "size: 2"),
          Box(addrInfo1, "size: 2"),
          Box(addrInfo2, "size: 2"),
          Box(addrInfo3, "size: 2"),
          Box(addrInfo4, "size: 1")).toSet
      val getBoxesResult = await(
        getBoxesEndpoint(GetBoxes
          .Args(Seq(addrInfo0, addrInfo1, addrInfo2, addrInfo3, addrInfo4), Byte.MinValue))).toSet

      val getBoxResult = await(getBoxEndpoint(GetBox.Args(addrInfo0, Byte.MinValue)))

      assert(getBoxesResult == expectedThreeNodes)
      assert(getBoxResult == Box(addrInfo0, "hi"))

      getBoxesEndpoint.close()
      getBoxEndpoint.close()
      servers.map(_.close())

    }
  }

  test("MethodBuilder with no partitioning strategy") {
    new MethodBuilderCtx {
      val getBoxesEndpoint = builder
        .servicePerEndpoint[DeliveryService.ServicePerEndpoint](methodName = "getBoxes")
        .getBoxes

      val expectedOneNode =
        Seq(Box(addrInfo1, "size: 3"), Box(addrInfo11, "size: 3"), Box(addrInfo2, "size: 3")).toSet

      val result =
        await(
          getBoxesEndpoint(
            GetBoxes.Args(Seq(addrInfo1, addrInfo2, addrInfo11), Byte.MinValue))).toSet

      assert(result == expectedOneNode)

      getBoxesEndpoint.close()
      servers.map(_.close())
    }
  }
}
