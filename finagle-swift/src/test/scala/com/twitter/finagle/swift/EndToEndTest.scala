package com.twitter.finagle.exp.swift

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.apache.thrift.TApplicationException
import com.twitter.finagle.Thrift
import com.twitter.util.{Future, Await}

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite {
  class Impl1 extends Test1 {
    def ab(a: String, b: java.util.Map[String, java.lang.Integer]) = {
      b.remove(a)
      Future.value(b)
    }

    def ping(x: String) = Future.value(x)
  }

  test("end to end, com.twitter.finagle.Thrift integration") {
    val impl = new Impl1

    val server = Thrift.serveIface(":*", impl)
    val client = Thrift.newIface[Test1](server)
    
    val map = new java.util.HashMap[String, java.lang.Integer]
    map.put("okay", 1234)
    map.put("nope", 4321)
    val map1 = Await.result(client.ab("okay", map))
    assert(map1.size === 1)
    assert(map1.get("okay") === null)
    assert(map1.get("nope") === 4321)
  }

  test("exceptions") {
    val impl = new Test1 {
      def ping(x: String) = Future.exception(new Exception("WTF"))
      def ab(a: String, b: java.util.Map[String, java.lang.Integer]) = 
        Future.exception(Test1Exc("hello, exceptional world", 123))
    }
    
    val server = Thrift.serveIface(":*", impl)
    val client = Thrift.newIface[Test1](server)
    
    val exc = intercept[TApplicationException] { Await.result(client.ping("ok")) }
    assert(exc.getMessage === "Internal error processing ping: 'java.lang.Exception: WTF'")
    
    val exc1 = intercept[Test1Exc] { Await.result(client.ab("blah", new java.util.HashMap)) }
    assert(exc1 === Test1Exc("hello, exceptional world", 123))
  }
}
