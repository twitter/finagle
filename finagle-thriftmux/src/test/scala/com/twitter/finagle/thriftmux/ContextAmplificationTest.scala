package com.twitter.finagle.thriftmux

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Address, Name, Service, ThriftMux}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.finagle.thriftmux.thriftscala.TestService.{Inquiry, Query, Question}
import com.twitter.io.Buf
import com.twitter.scrooge.{Request, Response}
import com.twitter.util.{Await, Awaitable, Duration, Future, Return, Try}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class ContextAmplificationTest extends AnyFunSuite with OneInstancePerTest {

  def await[T](a: Awaitable[T], d: Duration = 60.seconds): T =
    Await.result(a, d)

  protected def clientImpl: ThriftMux.Client =
    ThriftMux.client.copy(muxer = ThriftMux.Client.standardMuxer)

  protected def serverImpl: ThriftMux.Server = {
    // need to copy the params since the `.server` call sets the Label to "thrift" into
    // the current muxers params
    val serverParams = ThriftMux.server.params
    ThriftMux.server.copy(muxer = ThriftMux.Server.defaultMuxer.withParams(serverParams))
  }

  case class TestContext(buf: Buf)
  val testContext = new Contexts.broadcast.Key[TestContext]("com.twitter.finagle.mux.MuxContext") {
    def marshal(tc: TestContext): Buf = tc.buf
    def tryUnmarshal(buf: Buf): Try[TestContext] = Return(TestContext(buf))
  }

  val originServer = serverImpl.serveIface(
    new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
    new TestService.ReqRepServicePerEndpoint {
      def query: Service[Request[Query.Args], Response[String]] =
        Service.mk { req: Request[Query.Args] =>
          Future.value(Response(req.headers.toBufSeq.length.toString))
        }
      def question: Service[Request[Question.Args], Response[String]] = ???
      def inquiry: Service[Request[Inquiry.Args], Response[String]] = ???
    }.toThriftService
  )

  val proxyServer = {
    val proxyClient: TestService.MethodPerEndpoint = {
      val underlying = clientImpl.servicePerEndpoint[TestService.ServicePerEndpoint](
        Name.bound(Address(originServer.boundAddress.asInstanceOf[InetSocketAddress])),
        "ProxyClient"
      )
      // This sets up the auto-forwarding of request headers
      ThriftMux.Client.methodPerEndpoint(underlying)
    }

    serverImpl.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.ReqRepServicePerEndpoint {

        def query: Service[Request[Query.Args], Response[String]] = Service.mk {
          req: Request[Query.Args] =>
            val requestHeaders = req.headers.toBufSeq.length
            proxyClient.query("").map { result =>
              val transmittedHeaders = result.toInt
              if (transmittedHeaders == requestHeaders) Response("success")
              else Response(s"Unexpected number of headers transmitted: $transmittedHeaders")
            }
        }
        def question: Service[Request[Question.Args], Response[String]] = ???
        def inquiry: Service[Request[Inquiry.Args], Response[String]] = ???
      }.toThriftService
    )
  }

  test("contexts/headers are not amplified between hops") {
    val client =
      clientImpl.build[TestService.MethodPerEndpoint](
        Name.bound(Address(proxyServer.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    Contexts.broadcast.let(testContext, TestContext(Buf.Utf8("foo"))) {
      assert(await(client.query("ok").map { s => s }) == "success")
    }

    await(originServer.close(3.seconds))
  }
}
