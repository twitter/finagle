package com.twitter.finagle.http

import com.twitter.finagle.http.cookie.SameSite
import com.twitter.finagle.http.cookie.exp.supportSameSiteCodec
import com.twitter.finagle.toggle.flag
import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Duration, Future}
import java.net.InetSocketAddress
import org.scalatest.FunSuite

class SameSiteEndToEndTest extends FunSuite {

  class Ctx(useNetty4CookieCodec: Double = 0.0) {
    def setCookieservice(sameSite: SameSite): Service[Request, Response] =
      new Service[Request, Response] {
        def apply(req: Request): Future[Response] = {
          supportSameSiteCodec.let(true) {
            flag.overrides.let(Map(
              "com.twitter.finagle.http.UseNetty4CookieCodec" -> useNetty4CookieCodec
            )) {
              val response = Response(Status.Ok)
              response.cookies += new Cookie("foo", "bar").sameSite(sameSite)
              Future.value(response)
            }
          }
        }
    }

    def inspectCookieService(): Service[Request, Response] =
      new Service[Request, Response] {
        def apply(req: Request): Future[Response] = {
          supportSameSiteCodec.let(true) {
            flag.overrides.let(Map(
              "com.twitter.finagle.http.UseNetty4CookieCodec" -> useNetty4CookieCodec
            )) {
              val cookie = req.cookies.get("setBy").get
              val header = req.headerMap.get("Cookie").get
              val response = Response(Status.Ok)
              // Add a cookie to the response based on the content of the request cookie.
              // (Value should always be "nope".)
              if (cookie.sameSite == SameSite.Lax || header.contains("Lax")) {
                response.cookies += new Cookie("containsSameSite", "lax")
              } else if (cookie.sameSite == SameSite.Strict || header.contains("Strict")) {
                response.cookies += new Cookie("containsSameSite", "strict")
              } else {
                response.cookies += new Cookie("containsSameSite", "nope")
              }
              Future.value(response)
            }
          }
        }
    }

    val request = Request("/")
    val laxRequest = Request("/")
    supportSameSiteCodec.let(true) {
      flag.overrides.let(Map(
        "com.twitter.finagle.http.UseNetty4CookieCodec" -> useNetty4CookieCodec
      )) {
        val sameSiteCookie = new Cookie("setBy", "client").sameSite(SameSite.Lax)
        laxRequest.cookies += sameSiteCookie
      }
    }
    val strictRequest = Request("/")
    supportSameSiteCodec.let(true) {
      flag.overrides.let(Map(
        "com.twitter.finagle.http.UseNetty4CookieCodec" -> useNetty4CookieCodec
      )) {
        val sameSiteCookie = new Cookie("setBy", "client").sameSite(SameSite.Strict)
        strictRequest.cookies += sameSiteCookie
      }
    }
  }

  val timeout = Duration.fromSeconds(30)
  def await[A](f: Future[A]): A = Await.result(f, timeout)

  for {
    useNetty4CookieCodec <- Seq(0.0, 1.0)
  } {
    test(s"response should contain Lax if set in laxService, with useNetty4CookieCodec=$useNetty4CookieCodec") {
      supportSameSiteCodec.let(true) {
        new Ctx(useNetty4CookieCodec) {
          val server =
            Http.server
              .withLabel("myservice")
              .serve(new InetSocketAddress(0), setCookieservice(SameSite.Lax))

          val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
          val client = Http.client.newService(s":${ addr.getPort }", "http")

          val response: Response = await(client(request))
          assert(response.status == Status.Ok)
          assert(response.headerMap.get("Set-Cookie").get == "foo=bar; SameSite=Lax")
          assert(response.cookies.get("foo").get.sameSite == SameSite.Lax)
        }
      }
    }
  }

  for {
    useNetty4CookieCodec <- Seq(0.0, 1.0)
  } {
    test(s"response should contain Strict if set in strictService, with useNetty4CookieCodec=$useNetty4CookieCodec") {
      supportSameSiteCodec.let(true) {
        new Ctx(useNetty4CookieCodec) {
          val server =
            Http.server
              .withLabel("myservice")
              .serve(new InetSocketAddress(0), setCookieservice(SameSite.Strict))

          val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
          val client = Http.client.newService(s":${ addr.getPort }", "http")

          val response: Response = await(client(request))
          assert(response.status == Status.Ok)
          assert(response.headerMap.get("Set-Cookie").get == "foo=bar; SameSite=Strict")
          assert(response.cookies.get("foo").get.sameSite == SameSite.Strict)
        }
      }
    }
  }

  for {
    useNetty4CookieCodec <- Seq(0.0, 1.0)
  } {
    test(s"response should contain no SameSite attribute if set to None, with useNetty4CookieCodec=$useNetty4CookieCodec") {
      new Ctx(useNetty4CookieCodec) {
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
  }

  for {
    useNetty4CookieCodec <- Seq(0.0, 1.0)
  } {
    test(s"server should not see SameSite.Lax if set by client, with useNetty4CookieCodec=$useNetty4CookieCodec") {
      // Note that this test also confirms that the client does not see the
      // SameSite attribute in the response. The reason is that the attribute
      // is specified for only Set-Cookie, i.e., only for the response path:
      // =
      // https://tools.ietf.org/html/draft-west-first-party-cookies-07#section-3.1
      new Ctx(useNetty4CookieCodec) {
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
  }

  for {
    useNetty4CookieCodec <- Seq(0.0, 1.0)
  } {
    test(s"server should not see SameSite.Strict if set by client, with useNetty4CookieCodec=$useNetty4CookieCodec") {
      // See comment in previous test.
      new Ctx(useNetty4CookieCodec) {
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

}
