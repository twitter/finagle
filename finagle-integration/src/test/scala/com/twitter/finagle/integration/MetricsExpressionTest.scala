package com.twitter.finagle.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.integration.thriftscala.Echo
import com.twitter.finagle.service.MetricBuilderRegistry.ExpressionNames.deadlineRejectName
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.exp.ExpressionNames
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Duration
import com.twitter.util.Future
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import java.net.InetSocketAddress

class MetricsExpressionTest extends AnyFunSuite with BeforeAndAfter {

  val thriftService = new Echo.MethodPerEndpoint {
    def echo(msg: String): Future[String] = Future.value(msg)
  }

  val httpService = Service.mk { request: Request => Future.value(Response(request)) }

  private[this] def getAddress(server: ListeningServer): Name.Bound = {
    Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
  }
  private[this] def await[T](awaitable: Awaitable[T], timeout: Duration = 5.second): T =
    Await.result(awaitable, timeout)

  val keySet = Set(
    ExpressionNames.successRateName,
    ExpressionNames.throughputName,
    ExpressionNames.latencyName,
    ExpressionNames.failuresName,
    deadlineRejectName
  )

  val statsReceivers = List.fill(4)(new InMemoryStatsReceiver)

  test("stack params that contain stateful MetricBuilderRegistries are incoherent") {
    // create servers
    val thriftmuxServer0 =
      ThriftMux.server.withStatsReceiver(statsReceivers(0)).serveIface("localhost:*", thriftService)
    val thriftmuxServer1 =
      ThriftMux.server.withStatsReceiver(statsReceivers(1)).serveIface("localhost:*", thriftService)
    val thriftServer2 =
      Thrift.server.withStatsReceiver(statsReceivers(2)).serveIface("localhost:*", thriftService)
    val httpServer3 =
      Http.server.withStatsReceiver(statsReceivers(3)).serve("localhost:*", httpService)

    // expressions created at the stack materialization time
    assert(statsReceivers.forall { sr =>
      sr.expressions.size == 4 &&
      sr.expressions.mapValues(_.name).values.toSet == keySet - deadlineRejectName
    })

    // establish a connection -> deadline metrics and expressions initiated
    await(Http.client.newService(getAddress(httpServer3), "client0")(Request("/")))
    await(
      Thrift.client
        .build[Echo.MethodPerEndpoint](getAddress(thriftmuxServer0), "client1")
        .echo("hi"))
    await(
      Thrift.client
        .build[Echo.MethodPerEndpoint](getAddress(thriftmuxServer1), "client2")
        .echo("hi"))
    await(
      Thrift.client
        .build[Echo.MethodPerEndpoint](getAddress(thriftServer2), "client3")
        .echo("hi"))

    // all expressions are created
    assert(statsReceivers.forall { sr =>
      sr.expressions.size == 5 &&
      sr.expressions.mapValues(_.name).values.toSet == keySet
    })
  }

}
