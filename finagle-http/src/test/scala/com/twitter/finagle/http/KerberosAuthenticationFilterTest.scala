package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.param.KerberosConfiguration
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Await, Future, Promise}
import org.scalatest.FunSuite

class KerberosAuthenticationFilterTest extends FunSuite {
  private val filter =
    new AndThenAsync[Request, SpnegoAuthenticator.Authenticated[Request], Response](
      Spnego(KerberosConfiguration(Some("test-principal@twitter.biz"), Some("/keytab/path"))),
      ExtractAuthAndCatchUnauthorized)
  private val exampleService = new Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      val response = Response(request)
      Future.value(response)
    }
  }
  private def service: Service[Request, Response] = filter.andThen {
    Service.mk { exampleService }
  }
  private val request = Request("/test.json")
  request.method = Method.Get

  test("successfully test auth header response") {
    val response = Await.result(filter(request, service), 1.second)
    assert(response.headerMap.nonEmpty)
    assert(response.headerMap.get(Fields.WwwAuthenticate).contains("Negotiate"))
  }
}

class AndThenAsyncTest extends FunSuite {
  case class TestFilter(prefix: String, suffix: String) extends SimpleFilter[String, String] {
    def apply(
      request: String,
      service: Service[String, String]
    ): Future[String] = service(s"$prefix $request").map(x => s"$x $suffix")
  }
  val testService = new Service[String, String] {
    def apply(request: String): Future[String] = Future(request)
  }

  test("test async filter chained with other filter") {
    val p: Promise[SimpleFilter[String, String]] = Promise()
    val svc =
      new AndThenAsync[String, String, String](p, TestFilter("prefix2", "suffix2"))
        .andThen(testService)
    val andThenAsyncVal = svc("test")
    assert(!p.isDefined && !andThenAsyncVal.isDefined)

    p.setValue(TestFilter("prefix1", "suffix1"))
    val result = Await.result(andThenAsyncVal)
    assert(p.isDefined && result.nonEmpty)
    assert(result == "prefix2 prefix1 test suffix2 suffix1")
  }
}
