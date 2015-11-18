package com.twitter.finagle.http.codec

import com.twitter.finagle.{Failure, Dentry, Dtab, NameTree, Path}
import com.twitter.finagle.http.{Message, Method, Request, Version}
import com.twitter.util.Try
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class HttpDtabTest extends FunSuite with AssertionsForJUnit {
  val okDests = Vector("/$/inet/10.0.0.1/9000", "/foo/bar", "/")
  val okPrefixes = Vector("/foo", "/")
  val okDentries = for {
    prefix <- okPrefixes
    dest <- okDests
  } yield Dentry(Path.read(prefix), NameTree.read(dest))

  val okDtabs =
    Dtab.empty +: (okDentries.permutations map(ds => Dtab(ds))).toIndexedSeq

  def newMsg(): Message = Request(Version.Http11, Method.Get, "/")

  test("write(dtab, msg); read(msg) == dtab") {
    for (dtab <- okDtabs) {
      val m = newMsg()
      HttpDtab.write(dtab, m)
      val dtab1 = HttpDtab.read(m).get()
      assert(Equiv[Dtab].equiv(dtab, dtab1))
    }
  }

  test("Dtab-Local: read multiple, with commas") {
    val m = newMsg()
    m.headers.add("Dtab-Local", "/srv#/prod/local/role=>/$/fail;/srv=>/srv#/staging")
    m.headers.add("Dtab-Local", "/srv/local=>/srv/other,/srv=>/srv#/devel")
    val expected = Dtab.read(
      "/srv#/prod/local/role => /$/fail;"+
      "/srv => /srv#/staging;"+
      "/srv/local => /srv/other;"+
      "/srv => /srv#/devel"
    )
    assert(HttpDtab.read(m).get() == expected)
  }

  test("Dtab-Local takes precedence over X-Dtab") {
    val m = newMsg()
    m.headers.add("Dtab-Local", "/srv#/prod/local/role=>/$/fail;/srv=>/srv#/staging")
    // HttpDtab.write encodes X-Dtab headers
    HttpDtab.write(Dtab.read("/srv => /$/nil"), m)
    m.headers.add("Dtab-Local", "/srv/local=>/srv/other,/srv=>/srv#/devel")
    val expected = Dtab.read(
      "/srv => /$/nil;"+
      "/srv#/prod/local/role => /$/fail;"+
      "/srv => /srv#/staging;"+
      "/srv/local => /srv/other;"+
      "/srv => /srv#/devel"
    )
    assert(HttpDtab.read(m).get() == expected)
  }

  // some base64 encoders insert newlines to enforce max line length.  ensure we aren't doing that
  test("X-Dtab: long dest round-trips") {
    val expectedDtab = Dtab.read("/s/a => /s/abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz")
    val m = newMsg()
    HttpDtab.write(expectedDtab, m)
    val observedDtab = HttpDtab.read(m).get()
    assert(Equiv[Dtab].equiv(expectedDtab, observedDtab))
  }

  test("no headers") {
    val m = newMsg()
    assert(Equiv[Dtab].equiv(Dtab.empty, HttpDtab.read(m).get()))
  }

  test("X-Dtab: Invalid: no shared prefix") {
    val m = newMsg()
    m.headers.set("X-Dtab-01-A", "a")
    m.headers.set("X-Dtab-02-B", "a")
    val result = HttpDtab.read(m)
    val failure = intercept[Failure] { result.get() }
    assert(failure.why == "Unmatched X-Dtab headers")
  }

  test("X-Dtab: Invalid prefix") {
    val m = newMsg()
    m.headers.set("X-Dtab-01-A", "L2ZvbyA9PiAvZmFy") // /foo => /far
    m.headers.set("X-Dtab-01-B", "L2Zhcg==") // /far
    val result = HttpDtab.read(m)
    val failure = intercept[Failure] { result.get() }
    assert(failure.why == "Invalid path: /foo => /far")
  }

  test("X-Dtab: Invalid name") {
    val m = newMsg()
    m.headers.set("X-Dtab-01-A", "L2Zvbw==") // foo
    m.headers.set("X-Dtab-01-B", "L2ZvbyA9PiAvZmFy") // /foo => /far
    val result = HttpDtab.read(m)
    val failure = intercept[Failure] { result.get() }
    assert(failure.why == "Invalid name: /foo => /far")
  }

  test("X-Dtab: Invalid: missing entry") {
    val m = newMsg()
    m.headers.set("X-Dtab-01-A", "a")
    m.headers.set("X-Dtab-01-B", "a")
    m.headers.set("X-Dtab-02-B", "a")
    val result = HttpDtab.read(m)
    val failure = intercept[Failure] { result.get() }
    assert(failure.why == "Unmatched X-Dtab headers")
  }

  test("X-Dtab: Invalid: non-ASCII encoding") {
    val m = newMsg()
    m.headers.set("X-Dtab-01-A", "☺")
    m.headers.set("X-Dtab-01-B", "☹")
    val result = HttpDtab.read(m)
    val failure = intercept[Failure] { result.get() }
    assert(failure.why == "Value not b64-encoded: ☺")
  }

  test("clear()") {
    val m = newMsg()
    HttpDtab.write(Dtab.read("/a=>/b;/a=>/c"), m)
    m.headers.set("Dtab-Local", "/srv=>/srv#/staging")
    m.headers.set("onetwothree", "123")

    val headers = Seq(
      "X-Dtab-00-A", "X-Dtab-00-B",
      "X-Dtab-01-A", "X-Dtab-01-B",
      "Dtab-Local")

    for (h <- headers)
      assert(m.headers.contains(h), h+" not in headers")

    assert(m.headers.contains("onetwothree"), "onetwothree not in headers")

    HttpDtab.clear(m)

    assert(m.headers.contains("onetwothree"), "onetwothree was removed from headers")
    for (h <- headers)
      assert(!m.headers.contains(h), h+" was not removed from headers")
  }

  test("strip(msg)") {
    val dtabHeaders = Seq(
        ("Dtab-Local", "/srv=>/$/nil"),
        ("X-Dtab-00-A", "/srv#/prod/local/role"),
        ("X-Dtab-00-B", "/$/fail"),
        ("X-Dtab-01-A", "/srv/local"),
        ("X-Dtab-01-B", "/srv/other")
      )
    val allHeaders = dtabHeaders :+ (("Accept", "application/json"))

    val message = allHeaders.foldLeft(newMsg()) {
      (m, h) =>
        m.headers.set(h._1, h._2)
        m
    }

    val foundHeaders = HttpDtab.strip(message)

    assert(dtabHeaders == foundHeaders)
  }
}

