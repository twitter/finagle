package com.twitter.finagle.http

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParamMapTest extends FunSuite {
  test("no params") {
    val request = Request("/search.json")
    assert(request.params.get("q") == None)
  }

  test("no params, just question mark") {
    val request = Request("/search.json")
    assert(request.params.get("q") == None)
  }

  test("params") {
    val request = Request("/search.json?q=twitter")
    assert(request.params.get("q") == Some("twitter"))
  }

  test("getShort") {
    assert(Request("?x=1").params.getShort("x")       == Some(1.toShort))
    assert(Request("?x=0").params.getShort("x")       == Some(0.toShort))
    assert(Request("?x=-1").params.getShort("x")      == Some((-1).toShort))
    assert(Request("?x=32767").params.getShort("x")   == Some(32767.toShort))
    assert(Request("?x=-32768").params.getShort("x")  == Some((-32768).toShort))
    assert(Request("?x=32768").params.getShort("x")   == Some(0.toShort))
    assert(Request("?x=-32769").params.getShort("x")  == Some(0.toShort))
    assert(Request("?x=garbage").params.getShort("x") == Some(0.toShort))
    assert(Request("?x=").params.getShort("x")        == Some(0.toShort))
    assert(Request("?y=2").params.getShort("x")       == None)
  }

  test("getInt") {
    assert(Request("?x=1").params.getInt("x")           == Some(1))
    assert(Request("?x=0").params.getInt("x")           == Some(0))
    assert(Request("?x=-1").params.getInt("x")          == Some(-1))
    assert(Request("?x=2147483647").params.getInt("x")  == Some(2147483647))
    assert(Request("?x=-2147483648").params.getInt("x") == Some(-2147483648))
    assert(Request("?x=2147483648").params.getInt("x")  == Some(0))
    assert(Request("?x=-2147483649").params.getInt("x") == Some(0))
    assert(Request("?x=garbage").params.getInt("x")     == Some(0))
    assert(Request("?x=").params.getInt("x")            == Some(0))
    assert(Request("?y=2").params.getInt("x")           == None)
  }

  test("getLong") {
    assert(Request("?x=1").params.getLong("x")                    == Some(1L))
    assert(Request("?x=0").params.getLong("x")                    == Some(0L))
    assert(Request("?x=-1").params.getLong("x")                   == Some(-1L))
    assert(Request("?x=9223372036854775807").params.getLong("x")  == Some(9223372036854775807L))
    assert(Request("?x=-9223372036854775808").params.getLong("x") == Some(-9223372036854775808L))
    assert(Request("?x=9223372036854775808").params.getLong("x")  == Some(0L))
    assert(Request("?x=-9223372036854775809").params.getLong("x") == Some(0L))
    assert(Request("?x=garbage").params.getLong("x")              == Some(0L))
    assert(Request("?x=").params.getLong("x")                     == Some(0L))
    assert(Request("?y=2").params.getLong("x")                    == None)
  }

  test("getBoolean") {
    assert(Request("?x=true").params.getBoolean("x")    == Some(true))
    assert(Request("?x=TRUE").params.getBoolean("x")    == Some(true))
    assert(Request("?x=True").params.getBoolean("x")    == Some(true))
    assert(Request("?x=t").params.getBoolean("x")       == Some(true))
    assert(Request("?x=1").params.getBoolean("x")       == Some(true))
    assert(Request("?x=false").params.getBoolean("x")   == Some(false))
    assert(Request("?x=").params.getBoolean("x")        == Some(false))
    assert(Request("?x=garbage").params.getBoolean("x") == Some(false))
    assert(Request("?y=2").params.getBoolean("x")       == None)
  }

  test("params encoded") {
    val request = Request("/search.json?%71=%74%77%69%74%74%65%72")
    assert(request.params.get("q") == Some("twitter"))
  }

  test("params and equals encoded") {
    val request = Request("/search.json?%71%3D%74%77%69%74%74%65%72")
    assert(request.params.get("q") == None)
  }

  test("multiple params") {
    val request = Request("/search.json?q=twitter&lang=en")
    assert(request.params.get("q")    == Some("twitter"))
    assert(request.params.get("lang") == Some("en"))
  }

  test("key, no value") {
    val request = Request("/search.json?q=")
    assert(request.params.get("q") == Some(""))
  }

  test("value, no key is ignored") {
    val request = Request("/search.json?=value")
    assert(request.params.get("") == None)
  }

  test("favor first value") {
    val request = Request("/search.json?q=twitter&q=twitter2")
    assert(request.params.get("q") == Some("twitter"))
    assert(request.params.keys.toList == List("q"))
    assert(request.params.keySet.toList == List("q"))
    assert(request.params.keysIterator.toList == List("q"))
  }

  test("getAll") {
    val request = Request("/search.json?q=twitter&q=twitter2")
    assert(request.params.getAll("q").toList == List("twitter", "twitter2"))
  }

  test("iterator") {
    val request = Request("/search.json?q=twitter&q=twitter2&lang=en")
    assert(request.params.iterator.toList.sorted == List(("lang", "en"), ("q", "twitter"), ("q", "twitter2")))
  }

  test("plus") {
    val request = Request("/search.json?q=twitter")
    val params = request.params + ("lang" -> "en")
    assert(params.get("q")    == Some("twitter"))
    assert(params.get("lang") == Some("en"))
  }

  test("minus") {
    val request = Request("/search.json?q=twitter")
    val params = request.params - "q"
    assert(params.get("q") == None)
  }

  test("empty") {
    val request = Request("/search.json?q=twitter")
    val params = request.params.empty
    assert(params.get("q") == None)
  }

  test("toString") {
    assert(Request("/search.json?q=twitter").params.toString == "?q=twitter")
    assert(Request("/search.json").params.toString           == "")
  }

  test("get, POST params") {
    testPostParams(Method.Post)
    testPostParams(Method.Put)

    def testPostParams(method: Method): Unit = {
      val request = Request(method, "/search.json")
      request.contentType = "application/x-www-form-urlencoded"
      request.contentString = "q=twitter"
      assert(request.params.get("q") == Some("twitter"))
    }
  }

  test("getAll, POST params") {
    testPostParams(Method.Post)
    testPostParams(Method.Put)

    def testPostParams(method: Method): Unit = {
      val request = Request(method, "/search.json?q=twitter2")
      request.contentType = "application/x-www-form-urlencoded"
      request.contentString = "q=twitter"
      assert(request.params.get("q") == Some("twitter")) // favor POST param
      assert(request.params.getAll("q").toList.sorted == List("twitter", "twitter2"))
    }
  }

  test("ignore body only during TRACE requests") {
    val url = "/search.json?lang=en"
    val contentType = "application/x-www-form-urlencoded"
    val contentString = "q=twitter"

    val getRequest = Request(Method.Get, url)
    getRequest.contentType = contentType
    getRequest.contentString = contentString
    assert(getRequest.params.get("q") == Some("twitter"))
    assert(getRequest.params.get("lang") == Some("en"))

    val traceRequest = Request(Method.Trace, url)
    traceRequest.contentType = contentType
    traceRequest.contentString = contentString
    assert(traceRequest.params.get("q") == None)
    assert(traceRequest.params.get("lang") == Some("en"))
  }

  test("weird encoded characters") {
    for (i <- 0x7f until 0xff) {
      val getRequest = Request("/search.json?q=%%02x".format(i))
      assert(getRequest.params.get("q").nonEmpty == true)

      val postRequest = Request(Method.Post, "/search.json")
      postRequest.contentType = "application/x-www-form-urlencoded"
      postRequest.contentString = "q=%%02x".format(i)
      assert(postRequest.params.get("q").nonEmpty == true)
    }
  }

  test("illegal hex characters") {
    val request = Request("/search.json?q=%u3")
    assert(request.params.isValid == false)
  }

  test("incomplete trailing escape") {
    val request = Request("/search.json?q=%3")
    assert(request.params.isValid == false)
  }

  test("quotes are ok") {
    // Java's URL doesn't allow this, but we do.
    val request = Request("/search.json?q=\"twitter\"")
    assert(request.params.get("q") == Some("\"twitter\""))
  }
}
