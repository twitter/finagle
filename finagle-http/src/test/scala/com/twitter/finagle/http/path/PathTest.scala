package com.twitter.finagle.http.path

import com.twitter.finagle.http.{Method, ParamMap}
import com.twitter.finagle.http.path.{Path => FPath} // conflicts with spec's Path
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PathTest extends FunSuite {

  test("/foo/bar") {
    assert(FPath("/foo/bar").toList === List("foo", "bar"))
  }

  test("foo/bar") {
    assert(FPath("foo/bar").toList === List("foo", "bar"))
  }

  test(":? extractor") {
    assert {
      (FPath("/test.json") :? ParamMap()) match {
        case Root / "test.json" :? _ => true
        case _                       => false
      }
    }
  }

  test(":? extractor with ParamMatcher") {
    object A extends ParamMatcher("a")
    object B extends ParamMatcher("b")

    assert {
      (FPath("/test.json") :? ParamMap("a" -> "1", "b" -> "2")) match {
        case Root / "test.json" :? A(a) => a == "1"
        case _                          => false
      }
    }
    assert {
      (FPath("/test.json") :? ParamMap("a" -> "1", "b" -> "2")) match {
        case Root / "test.json" :? B(b) => b == "2"
        case _                          => false
      }
    }
    assert {
      (FPath("/test.json") :? ParamMap("a" -> "1", "b" -> "2")) match {
        case Root / "test.json" :? (A(a) :& B(b)) => a == "1" && b == "2"
        case _                                    => false
      }
    }
    assert {
      (FPath("/test.json") :? ParamMap("a" -> "1", "b" -> "2")) match {
        case Root / "test.json" :? (B(b) :& A(a)) => a == "1" && b == "2"
        case _                                    => false
      }
    }
  }

  test(":? extractor with IntParamMatcher and LongParamMatcher") {
    object I extends IntParamMatcher("i")
    object L extends LongParamMatcher("l")
    object D extends DoubleParamMatcher("d")

    assert {
      (FPath("/test.json") :? ParamMap("i" -> "1", "l" -> "2147483648", "d" -> "1.3")) match {
        case Root / "test.json" :? (I(i) :& L(l) :& D(d)) => i == 1 && l == 2147483648L && d == 1.3D
        case _                                            => false
      }
    }
  }

  test("~ extractor on Path") {
    assert {
      FPath("/foo.json") match {
        case Root / "foo" ~ "json" => true
        case _                     => false
      }
    }
  }

  test("~ extractor on filename foo.json") {
    assert {
      "foo.json" match {
        case "foo" ~ "json" => true
        case _              => false
      }
    }
  }

  test("~ extractor on filename foo") {
    assert {
      "foo" match {
        case "foo" ~ "" => true
        case _          => false
      }
    }
  }

  test("-> extractor") {
    assert {
      (Method.Get, FPath("/test.json")) match {
        case Method.Get -> Root / "test.json" => true
        case _                                => false
      }
    }
  }

  test("Root extractor") {
    assert {
      FPath("/") match {
        case Root => true
        case _    => false
      }
    }
  }

  test("Root extractor, no partial match") {
    assert {
      (FPath("/test.json") match {
        case Root => true
        case _    => false
      }) === false
    }
  }

  test("Root extractor, empty path") {
    assert {
      FPath("") match {
        case Root => true
        case _    => false
      }
    }
  }

  test("/ extractor") {
    assert {
      FPath("/1/2/3/test.json") match {
        case Root / "1" / "2" / "3" / "test.json" => true
        case _                                    => false
      }
    }
  }

  test("Integer extractor") {
    assert {
      FPath("/user/123") match {
        case Root / "user" / Integer(userId) => userId == 123
        case _                               => false
      }
    }
  }

  test("Integer extractor, negative int")  {
    assert {
      FPath("/user/-123") match {
        case Root / "user" / Integer(userId) => userId == -123
        case _                               => false
      }
    }
  }

  test("Integer extractor, invalid int") {
    assert {
      (FPath("/user/invalid") match {
        case Root / "user" / Integer(userId) => true
        case _                               => false
      }) === false
    }
  }

  test("Integer extractor, number format error") {
    assert {
      (FPath("/user/2147483648") match {
        case Root / "user" / Integer(userId) => true
        case _                               => false
      }) === false
    }
  }

  test("Long extractor") {
    assert {
      FPath("/user/123") match {
        case Root / "user" / Long(userId) => userId == 123
        case _                            => false
      }
    }
  }

  test("Long extractor, invalid int") {
    assert {
      (FPath("/user/invalid") match {
        case Root / "user" / Long(userId) => true
        case _                            => false
      }) === false
    }
  }

  test("Long extractor, number format error") {
    assert {
      (FPath("/user/9223372036854775808") match {
        case Root / "user" / Long(userId) => true
        case _                            => false
      }) === false
    }
  }
}
