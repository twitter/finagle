package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.KerberosAuthenticationFilter.{
  ExtractAuthAndCatchUnauthorized,
  SpnegoClientFilter,
  SpnegoServerFilter
}
import com.twitter.finagle.http.param.{ClientKerberosConfiguration, ServerKerberosConfiguration}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Await, Future, Promise}
import java.nio.file.{FileSystems, Files}
import org.scalatest.funsuite.AnyFunSuite

class KerberosAuthenticationFilterTest extends AnyFunSuite {
  private val serverFilter =
    new AsyncFilter[Request, SpnegoAuthenticator.Authenticated[Request], Response](
      SpnegoServerFilter(
        ServerKerberosConfiguration(Some("test-principal@twitter.biz"), Some("/keytab/path"))))
      .andThen(ExtractAuthAndCatchUnauthorized)
  private val clientFilter =
    new AsyncFilter[Request, Request, Response](
      SpnegoClientFilter(
        ClientKerberosConfiguration(
          Some("test-principal@twitter.biz"),
          Some("/keytab/path"),
          Some("test-server-principal@twitter.biz"))))
  private val exampleService = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      val response = Response(request)
      Future.value(response)
    }
  }
  private def serverService: Service[Request, Response] =
    serverFilter.andThen {
      Service.mk { exampleService }
    }
  private def clientService: Service[Request, Response] = clientFilter.andThen {
    Service.mk { exampleService }
  }
  private val request = Request("/test.json")
  request.method = Method.Get

  test("successfully test server auth header response") {
    val response = Await.result(serverFilter(request, serverService), 1.second)
    assert(response.headerMap.nonEmpty)
    assert(response.headerMap.get(Fields.WwwAuthenticate).contains("Negotiate"))
  }

  test("successfully test authenticated http client") {
    val response = Await.result(clientFilter(request, clientService), 1.second)
    val jaasFilePath = FileSystems.getDefault.getPath("jaas-internal.conf").toAbsolutePath
    assert(Files.exists(jaasFilePath))
    assert(Files.lines(jaasFilePath).anyMatch(line => line.equals("kerberos-http-client {")))
    assert(response.statusCode == 200)
  }
}

class AsyncFilterTest extends AnyFunSuite {
  case class TestFilter(prefix: String, suffix: String) extends SimpleFilter[String, String] {
    def apply(
      request: String,
      service: Service[String, String]
    ): Future[String] = service(s"$prefix $request").map(x => s"$x $suffix")
  }
  val testService = new Service[String, String] {
    def apply(request: String): Future[String] = Future(request)
  }

  test("test async filter executed successfully") {
    val p: Promise[SimpleFilter[String, String]] = Promise()
    val svc =
      new AsyncFilter[String, String, String](p)
        .andThen(testService)
    val andThenAsyncVal = svc("test")
    assert(!p.isDefined && !andThenAsyncVal.isDefined)

    p.setValue(TestFilter("prefix1", "suffix1"))
    val result = Await.result(andThenAsyncVal)
    assert(p.isDefined && result.nonEmpty)
    assert(result == "prefix1 test suffix1")
  }
}
