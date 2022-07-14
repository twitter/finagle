package com.twitter.finagle.client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.service.FailedService
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Time
import java.net.InetSocketAddress
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ExceptionRemoteInfoFactoryTest extends AnyFunSuite with MockitoSugar {
  test(
    "ExceptionRemoteInfoFactory should add remote info to HasRemoteInfo service acquisition exceptions"
  ) {
    val failingFactory = new ServiceFactory[String, String] {
      def apply(conn: ClientConnection): Future[Nothing] = Future.exception(new HasRemoteInfo {})
      def close(deadline: Time): Future[Unit] = Future.Done
      def status: Status = Status.Open
    }

    val downstreamAddr = new InetSocketAddress("1.2.3.4", 100)
    val downstreamId = "downstream"
    val upstreamAddr = new InetSocketAddress("2.3.4.5", 100)
    val traceId = Trace.id

    val composed = new ExceptionRemoteInfoFactory(failingFactory, downstreamAddr, downstreamId)
    val actual = intercept[HasRemoteInfo] {
      Trace.letId(traceId, true) {
        ExceptionRemoteInfoFactory.letUpstream(Some(upstreamAddr), Some("upstream")) {
          Await.result(composed(), 1.second)
        }
      }
    }
    assert(
      actual.remoteInfo == RemoteInfo.Available(
        Some(upstreamAddr),
        Some("upstream"),
        Some(downstreamAddr),
        Some("downstream"),
        traceId
      )
    )
  }

  test("ExceptionRemoteInfoFactory should add remote info to request exceptions") {
    val serviceFactory =
      ServiceFactory.const[Int, Nothing](new FailedService(new HasRemoteInfo {}))

    val downstreamAddr = new InetSocketAddress("1.2.3.4", 100)
    val downstreamId = "downstream"
    val upstreamAddr = new InetSocketAddress("2.3.4.5", 100)
    val traceId = Trace.id

    val composed =
      new ExceptionRemoteInfoFactory[Int, Nothing](serviceFactory, downstreamAddr, downstreamId)
    val service = Await.result(composed(), 1.second)
    val actual = intercept[HasRemoteInfo] {
      Trace.letId(traceId, true) {
        ExceptionRemoteInfoFactory.letUpstream(Some(upstreamAddr), Some("upstream")) {
          Await.result(service.apply(0), 1.second)
        }
      }
    }
    assert(
      actual.remoteInfo == RemoteInfo.Available(
        Some(upstreamAddr),
        Some("upstream"),
        Some(downstreamAddr),
        Some("downstream"),
        traceId
      )
    )
  }

  test("ExceptionRemoteInfoFactory should add remote info to Failures") {
    val serviceFactory =
      ServiceFactory.const[Int, Nothing](new FailedService(new Failure("bad time")))

    val downstreamAddr = new InetSocketAddress("1.2.3.4", 100)
    val downstreamId = "downstream"
    val upstreamAddr = new InetSocketAddress("2.3.4.5", 100)
    val traceId = Trace.id

    val composed = new ExceptionRemoteInfoFactory(serviceFactory, downstreamAddr, downstreamId)
    val service = Await.result(composed(), 1.second)
    val actual = intercept[Failure] {
      Trace.letId(traceId, true) {
        ExceptionRemoteInfoFactory.letUpstream(Some(upstreamAddr), Some("upstream")) {
          Await.result(service.apply(0), 1.second)
        }
      }
    }
    assert(
      actual
        .getSource(Failure.Source.RemoteInfo)
        .contains(
          RemoteInfo.Available(
            Some(upstreamAddr),
            Some("upstream"),
            Some(downstreamAddr),
            Some("downstream"),
            traceId
          )
        )
    )
  }
}
