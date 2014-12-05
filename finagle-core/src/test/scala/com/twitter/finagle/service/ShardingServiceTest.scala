package com.twitter.finagle.service

import com.twitter.finagle.Status
import com.twitter.finagle.{NotShardableException, ShardNotAvailableException, Service}
import com.twitter.hashing.Distributor
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ShardingServiceTest extends FunSuite with MockitoSugar {

  class MockRequest
  class ShardingRequest(key: Long) extends MockRequest {
    def shardingKey = key
  }

  class ShardingServiceHelper {
    val distributor = mock[Distributor[Service[MockRequest, String]]]
    val service = new ShardingService(distributor, {
      request: MockRequest =>
        request match {
          case req: ShardingRequest => Some(req.shardingKey)
          case _ => None
        }
    })

    val reqA = new ShardingRequest(1L)
    val serviceForA = mock[Service[MockRequest, String]]
    when(serviceForA.close(any)) thenReturn Future.Done

    val unshardableReq = new MockRequest
    val reply = Future.value("hello")
  }

  test("ShardingService should distribute requests between two shards") {
    val h = new ShardingServiceHelper
    import h._

    val reqB = new ShardingRequest(2L)
    val serviceForB = mock[Service[MockRequest, String]]
    when(serviceForB.close(any)) thenReturn Future.Done

    when(distributor.nodeForHash(1L)) thenReturn serviceForA
    when(serviceForA.status) thenReturn Status.Open
    when(serviceForA.apply(reqA)) thenReturn reply
    service(reqA)
    verify(serviceForA).apply(reqA)

    when(distributor.nodeForHash(2L)) thenReturn serviceForB
    when(serviceForB.status) thenReturn Status.Open
    when(serviceForB.apply(reqB)) thenReturn reply
    service(reqB)
    verify(serviceForB).apply(reqB)
  }

  test("ShardingService should thenReturn an exception if the shard picked is unavailable") {
    val h = new ShardingServiceHelper
    import h._

    when(distributor.nodeForHash(1L)) thenReturn serviceForA
    when(serviceForA.status) thenReturn Status.Closed
    intercept[ShardNotAvailableException] {
      Await.result(service(reqA))
    }
    verify(serviceForA, times(0)).apply(reqA)
  }

  test("ShardingService should thenReturn an unshardable if the request is not shardable") {
    val h = new ShardingServiceHelper
    import h._

    intercept[NotShardableException] {
      Await.result(service(unshardableReq))
    }
    verify(distributor, times(0)).nodeForHash(anyLong)
  }

}
