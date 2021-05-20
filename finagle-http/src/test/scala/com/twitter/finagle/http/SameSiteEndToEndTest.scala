package com.twitter.finagle.http

import com.twitter.finagle.http.cookie.{SameSite, supportSameSiteCodec}
import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Duration, Future}
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class SameSiteEndToEndTest extends AnyFunSuite {

  class Ctx {
    def setCookieservice(sameSite: SameSite): Service[Request, Response] =
      new Service[Request, Response] {
        def apply(req: Request): Future[Response] = {
          supportSameSiteCodec.let(true) {
            val response = Response(Status.Ok)
            response.cookies += new Cookie("foo", "bar").sameSite(sameSite)
            Future.value(response)
          }
        }
      }

    def inspectCookieService(): Service[Request, Response] =
      new Service[Request, Response] {
        def apply(req: Request): Future[Response] = {
          supportSameSiteCodec.let(true) {
            val cookie = req.cookies.get("setBy").get
            val header = req.headerMap.get("Cookie").get
            val response = Response(Status.Ok)
            // Add a cookie to the response based on the content of the request cookie.
            if (cookie.sameSite == SameSite.Lax || header.contains("Lax")) {
              response.cookies += new Cookie("containsSameSite", "lax")
            } else if (cookie.sameSite == SameSite.Strict || header.contains("Strict")) {
              response.cookies += new Cookie("containsSameSite", "strict")
            } else if (cookie.sameSite == SameSite.None || header.contains("none")) {
              response.cookies += new Cookie("containsSameSite", "none")
            } else {
              response.cookies += new Cookie("containsSameSite", "nope")
            }
            Future.value(response)
          }
        }
      }

    val request = Request("/")
    val laxRequest = Request("/")
    supportSameSiteCodec.let(true) {
      val sameSiteCookie = new Cookie("setBy", "client").sameSite(SameSite.Lax)
      laxRequest.cookies += sameSiteCookie
    }
    val strictRequest = Request("/")
    supportSameSiteCodec.let(true) {
      val sameSiteCookie = new Cookie("setBy", "client").sameSite(SameSite.Strict)
      strictRequest.cookies += sameSiteCookie
    }
  }

  val timeout = Duration.fromSeconds(30)
  def await[A](f: Future[A]): A = Await.result(f, timeout)

  test("response should contain Lax if set in laxService") {
    supportSameSiteCodec.let(true) {
      new Ctx {
        val server =
          Http.server
            .withLabel("myservice")
            .serve(new InetSocketAddress(0), setCookieservice(SameSite.Lax))

        val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
        val client = Http.client.newService(s":${addr.getPort}", "http")

        val response: Response = await(client(request))
        assert(response.status == Status.Ok)
        assert(response.headerMap.get("Set-Cookie").get == "foo=bar; SameSite=Lax")
        assert(response.cookies.get("foo").get.sameSite == SameSite.Lax)
      }
    }
  }

  test("response should contain Strict if set in strictService") {
    supportSameSiteCodec.let(true) {
      new Ctx {
        val server =
          Http.server
            .withLabel("myservice")
            .serve(new InetSocketAddress(0), setCookieservice(SameSite.Strict))

        val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
        val client = Http.client.newService(s":${addr.getPort}", "http")

        val response: Response = await(client(request))
        assert(response.status == Status.Ok)
        assert(response.headerMap.get("Set-Cookie").get == "foo=bar; SameSite=Strict")
        assert(response.cookies.get("foo").get.sameSite == SameSite.Strict)
      }
    }
  }

  test("response should contain None if set in strictService") {
    supportSameSiteCodec.let(true) {
      new Ctx {
        val server =
          Http.server
            .withLabel("myservice")
            .serve(new InetSocketAddress(0), setCookieservice(SameSite.None))

        val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
        val client = Http.client.newService(s":${addr.getPort}", "http")

        val response: Response = await(client(request))
        assert(response.status == Status.Ok)
        assert(response.headerMap.get("Set-Cookie").get == "foo=bar; SameSite=None")
        assert(response.cookies.get("foo").get.sameSite == SameSite.None)
      }
    }
  }

  test("response should contain no SameSite attribute if set to Unset") {
    new Ctx {
      val server =
        Http.server
          .withLabel("myservice")
          .serve(new InetSocketAddress(0), setCookieservice(SameSite.Unset))

      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = Http.client.newService(s":${addr.getPort}", "http")

      val response: Response = await(client(request))
      assert(response.status == Status.Ok)
      assert(response.headerMap.get("Set-Cookie").get == "foo=bar")
      assert(response.cookies.get("foo").get.sameSite == SameSite.Unset)
    }
  }

  test("server should not see SameSite.Lax if set by client") {
    // Note that this test also confirms that the client does not see the
    // SameSite attribute in the response. The reason is that the attribute
    // is specified for only Set-Cookie, i.e., only for the response path:
    // =
    // https://tools.ietf.org/html/draft-west-first-party-cookies-07#section-3.1
    new Ctx {
      val server =
        Http.server
          .withLabel("myservice")
          .serve(new InetSocketAddress(0), inspectCookieService())

      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = Http.client.newService(s":${addr.getPort}", "http")

      val response: Response = await(client(laxRequest))
      assert(response.status == Status.Ok)
      assert(response.headerMap.get("Set-Cookie").get == "containsSameSite=nope")
      assert(response.cookies.get("containsSameSite").get.value == "nope")
    }
  }

  test("server should not see SameSite.Strict if set by client") {
    // See comment in previous test.
    new Ctx {
      val server =
        Http.server
          .withLabel("myservice")
          .serve(new InetSocketAddress(0), inspectCookieService())

      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = Http.client.newService(s":${addr.getPort}", "http")

      val response: Response = await(client(strictRequest))
      assert(response.status == Status.Ok)
      assert(response.headerMap.get("Set-Cookie").get == "containsSameSite=nope")
      assert(response.cookies.get("containsSameSite").get.value == "nope")
    }
  }

}
