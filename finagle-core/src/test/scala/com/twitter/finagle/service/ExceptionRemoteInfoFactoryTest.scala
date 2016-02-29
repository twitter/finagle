package com.twitter.finagle.service

import java.net.InetSocketAddress

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.context.{Contexts, RemoteInfo}
import com.twitter.finagle.thrift.ClientId
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Await, Future, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ExceptionRemoteInfoFactoryTest extends FunSuite with MockitoSugar {
  test("ExceptionRemoteInfoFactory should add remote info to HasRemoteInfo service acquisition exceptions") {
    val serviceFactory = ServiceFactory.const(new FailedService(new HasRemoteInfo {}))

    val failingFactory = new ServiceFactory[String, String] {
      def apply(conn: ClientConnection) = Future.exception(new HasRemoteInfo {})
      def close(deadline: Time) = Future.Done
    }

    val downstreamAddr = new InetSocketAddress("1.2.3.4", 100)
    val downstreamId = "downstream"
    val upstreamAddr = new InetSocketAddress("2.3.4.5", 100)
    val traceId = Trace.id

    val composed = new ExceptionRemoteInfoFactory(failingFactory, downstreamAddr, downstreamId)
    val actual = intercept[HasRemoteInfo] {
      Trace.letId(traceId, true) {
        Contexts.local.let(RemoteInfo.Upstream.AddressCtx, upstreamAddr) {
          ClientId.let(ClientId("upstream")) {
            Await.result(composed(), 1.second)
          }
        }
      }
    }
    assert(actual.remoteInfo == RemoteInfo.Available(
      Some(upstreamAddr), Some(ClientId("upstream")), Some(downstreamAddr), Some(ClientId("downstream")), traceId))
  }

  test("ExceptionRemoteInfoFactory should add remote info to request exceptions") {
    val serviceFactory = ServiceFactory.const(new FailedService(new HasRemoteInfo {}))

    val downstreamAddr = new InetSocketAddress("1.2.3.4", 100)
    val downstreamId = "downstream"
    val upstreamAddr = new InetSocketAddress("2.3.4.5", 100)
    val traceId = Trace.id

    val composed = new ExceptionRemoteInfoFactory(serviceFactory, downstreamAddr, downstreamId)
    val service = Await.result(composed(), 1.second)
    val actual = intercept[HasRemoteInfo] {
      Trace.letId(traceId, true) {
        Contexts.local.let(RemoteInfo.Upstream.AddressCtx, upstreamAddr) {
          ClientId.let(ClientId("upstream")) {
            Await.result(service.apply(0), 1.second)
          }
        }
      }
    }
    assert(actual.remoteInfo == RemoteInfo.Available(
      Some(upstreamAddr), Some(ClientId("upstream")), Some(downstreamAddr), Some(ClientId("downstream")), traceId))
  }

  test("ExceptionRemoteInfoFactory should add remote info to Failures") {
    val serviceFactory = ServiceFactory.const(new FailedService(new Failure("bad time")))

    val downstreamAddr = new InetSocketAddress("1.2.3.4", 100)
    val downstreamId = "downstream"
    val upstreamAddr = new InetSocketAddress("2.3.4.5", 100)
    val traceId = Trace.id

    val composed = new ExceptionRemoteInfoFactory(serviceFactory, downstreamAddr, downstreamId)
    val service = Await.result(composed(), 1.second)
    val actual = intercept[Failure] {
      Trace.letId(traceId, true) {
        Contexts.local.let(RemoteInfo.Upstream.AddressCtx, upstreamAddr) {
          ClientId.let(ClientId("upstream")) {
            Await.result(service.apply(0), 1.second)
          }
        }
      }
    }
    assert(actual.getSource(Failure.Source.RemoteInfo) == Some(RemoteInfo.Available(
      Some(upstreamAddr), Some(ClientId("upstream")), Some(downstreamAddr), Some(ClientId("downstream")), traceId)))
  }
}
