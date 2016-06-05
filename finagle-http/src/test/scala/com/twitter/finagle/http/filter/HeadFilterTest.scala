package com.twitter.finagle.http.filter

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HeadFilterTest extends FunSuite {
  val Body = "hello world"

  val dummyService = new Service[Request, Response] {
   def apply(request: Request) = {
     assert(request.method == Method.Get)

     val response = request.response
     response.status = Status.Ok
     response.write(Body)
     Future.value(response)
   }
  }

  test("convert GET to HEAD") {
    val request = Request("/test.json")
    request.method = Method.Head

    val response = Await.result(HeadFilter(request, dummyService))
    assert(request.method == Method.Head) // unchanged
    assert(response.contentLength == Some(Body.length))
    assert(response.contentString == "")
  }

  test("GET is normal") {
    val request = Request("/test.json")

    val response = Await.result(HeadFilter(request, dummyService))
    request.method == Method.Get // unchanged
    assert(response.contentLength == None)
    assert(response.contentString == Body)
  }
}
