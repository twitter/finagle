package com.twitter.finagle.service

import com.twitter.hashing._
import com.twitter.finagle.{Service, ShardNotAvailableException, NotShardableException}
import com.twitter.util.Future
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.mockito.Matchers._


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

    val unshardableReq = new MockRequest
    val reply = Future.value("hello")

    "distribute requests between two shards" in {
      val reqB = new ShardingRequest(2L)
      val serviceForB = mock[Service[MockRequest, String]]

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
      service(reqA)() must throwA[ShardNotAvailableException]
      there was no(serviceForA).apply(reqA)
    }

    "returns an unshardable if the request is not shardable" in {
      service(unshardableReq)() must throwA[NotShardableException]
      there was no(distributor).nodeForHash(anyLong)
    }
  }
}
