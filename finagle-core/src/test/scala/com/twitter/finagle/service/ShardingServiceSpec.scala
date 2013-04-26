package com.twitter.finagle.service

import com.twitter.finagle.{NotShardableException, Service, ShardNotAvailableException}
import com.twitter.hashing._
import com.twitter.util.{Await, Future}
import org.mockito.Matchers._
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito


class ShardingServiceSpec extends SpecificationWithJUnit with Mockito {
  "ShardingService" should {
    class MockRequest
    class ShardingRequest(key: Long) extends MockRequest {
      def shardingKey = key
    }

    val distributor = mock[Distributor[Service[MockRequest, String]]]
    val service = new ShardingService(distributor, { request: MockRequest =>
      request match {
        case req: ShardingRequest => Some(req.shardingKey)
        case _ => None
      }
    })

    val reqA = new ShardingRequest(1L)
    val serviceForA = mock[Service[MockRequest, String]]
    serviceForA.close(any) returns Future.Done

    val unshardableReq = new MockRequest
    val reply = Future.value("hello")

    "distribute requests between two shards" in {
      val reqB = new ShardingRequest(2L)
      val serviceForB = mock[Service[MockRequest, String]]
      serviceForB.close(any) returns Future.Done

      distributor.nodeForHash(1L) returns serviceForA
      serviceForA.isAvailable returns true
      serviceForA.apply(reqA) returns reply
      service(reqA)
      there was one(serviceForA).apply(reqA)

      distributor.nodeForHash(2L) returns serviceForB
      serviceForB.isAvailable returns true
      serviceForB.apply(reqB) returns reply
      service(reqB)
      there was one(serviceForB).apply(reqB)
    }

    "returns an exception if the shard picked is unavailable" in {
      distributor.nodeForHash(1L) returns serviceForA
      serviceForA.isAvailable returns false
      Await.result(service(reqA)) must throwA[ShardNotAvailableException]
      there was no(serviceForA).apply(reqA)
    }

    "returns an unshardable if the request is not shardable" in {
      Await.result(service(unshardableReq)) must throwA[NotShardableException]
      there was no(distributor).nodeForHash(anyLong)
    }
  }
}
