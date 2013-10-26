package com.twitter.finagle.http.codec

import com.twitter.finagle.{Dentry, Dtab}
import org.jboss.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class HttpDtabTest extends FunSuite {
  val okDests = Vector("inet!10.0.0.1:9000", "/foo/bar", "/")
  val okPrefixes = Vector("/foo", "/")
  val okDentries = for {
    prefix <- okPrefixes
    dest <- okDests
  } yield Dentry(prefix, dest)

  val okDtabs = 
    Dtab.empty +: (okDentries.permutations map(ds => Dtab(ds))).toIndexedSeq
  
  def newMsg(): HttpMessage =
    new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")

  test("write(dtab, msg); read(msg) == dtab") {
    for (dtab <- okDtabs) {
      val m = newMsg()
      HttpDtab.write(dtab, m)
      val dtab1 = HttpDtab.read(m)
      assert(Dtab.equiv(dtab, dtab1))
    }
  }
  
  test("Invalid: no shared prefix") {
    val m = newMsg()
    m.setHeader("X-Dtab-01-A", "a")
    m.setHeader("X-Dtab-02-B", "a")
    assert(HttpDtab.read(m) === Dtab.empty)
  }
  
  test("Invalid: missing entry") {
    val m = newMsg()
    m.setHeader("X-Dtab-01-A", "a")
    m.setHeader("X-Dtab-01-B", "a")
    m.setHeader("X-Dtab-02-B", "a")
    assert(HttpDtab.read(m) === Dtab.empty)
  }
  
  test("Invalid: non-ASCII encoding") {
    val m = newMsg()
    m.setHeader("X-Dtab-01-A", "☺")
    m.setHeader("X-Dtab-01-B", "☹")
    assert(HttpDtab.read(m) == Dtab.empty)
  }  
  
  test("clear()") {
    val m = newMsg()
    HttpDtab.write(Dtab.empty.delegated("/a", "/b").delegated("/a", "/c"), m)
    m.setHeader("onetwothree", "123")
    
    val headers = Seq(
      "X-Dtab-00-A", "X-Dtab-00-B", 
      "X-Dtab-01-A", "X-Dtab-01-B")
      
    for (h <- headers)
      assert(m.containsHeader(h))
    
    assert(m.containsHeader("onetwothree"))
    
    HttpDtab.clear(m)
    
    assert(m.containsHeader("onetwothree"))
    for (h <- headers)
      assert(!m.containsHeader(h))
  }


}
