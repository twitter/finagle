package com.twitter.finagle.mux

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.io.Buf
import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class CrossVersionEndToEndTest extends FunSuite {
  test("various netty implementations") {
    val muxEchoService = Service.mk[Request, Response] { req =>
      Future.value(Response(req.body))
    }

    val baseServer: Mux.Server = Mux.server
    val servers: Seq[Mux.Server] = Seq(
      baseServer.configured(Mux.param.MuxImpl.Netty3),
      baseServer.configured(Mux.param.MuxImpl.Netty4))

    val baseClient: Mux.Client = Mux.client
    val clients: Seq[Mux.Client] = Seq(
      baseClient.configured(Mux.param.MuxImpl.Netty3),
      baseClient.configured(Mux.param.MuxImpl.Netty4))

    for (server <- servers; client <- clients) {
      val srv = server.serve("localhost:*", muxEchoService)
      val clnt = client.newService(srv)

      val req = clnt(Request(Path.empty, Buf.Utf8("hello world")))
      assert(Await.result(req, 5.seconds) == Response(Buf.Utf8("hello world")))

      Await.result(srv.close(), 5.seconds)
      Await.result(clnt.close(), 5.seconds)
    }
  }

  test("Mux object respects netty transport toggle") {
    val clientParams = Mux.client.params
    val serverParams = Mux.server.params

    def assertImpl(prefix: String) = {
      assert(clientParams[Mux.param.MuxImpl].transporter(Stack.Params.empty).toString ==
        s"${prefix}Transporter")
      assert(serverParams[Mux.param.MuxImpl].listener(Stack.Params.empty).toString ==
        s"${prefix}Listener")
    }

    assertImpl("Netty3")

    toggle.flag.overrides.let("com.twitter.finagle.mux.UseNetty4", 1.0) {
      assertImpl("Netty4")
    }
  }
}
