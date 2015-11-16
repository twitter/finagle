package com.twitter.finagle.http.path

import com.twitter.finagle.http.{Method, ParamMap}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.Gen
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class PathTest extends FunSuite with GeneratorDrivenPropertyChecks {

  def alpha(min: Int, max: Int) = for {
    len <- Gen.choose(min, max)
  } yield Random.alphanumeric.take(len).mkString

  val pathParts = Gen.listOf[String](alpha(0, 10))

  test("construct by list") {
    forAll(pathParts) { (parts: List[String]) =>
      val p = Path(parts)
      assert(p.toList.length == parts.length)
      assert(p.lastOption == util.Try(parts.last).toOption)
      assert(p.startsWith(util.Try(Path(parts.init)).getOrElse(Root)))
      if (p != Root) assert(p.toString == parts.mkString("/", "/", ""))
    }
  }

  test("path separator extrator") {
    forAll(pathParts) { (parts: List[String]) =>
      val p = Path(parts)
      assert(/:.unapply(p) == util.Try(parts.head -> Path(parts.tail)).toOption)
    }
  }

  test("file extension extractor") {
    forAll(pathParts, alpha(0, 3)) { (parts: List[String], ext: String) =>
      whenever (parts.length > 0) {
        if (ext.length == 0) {
          val p = Path(parts)
          assert($tilde.unapply(p) == Some(p, ""))
        } else {
          val p = Path(parts.init ++ List(parts.last + "." + ext))
          assert($tilde.unapply(p) == Some(Path(parts), ext))
        }
      }
    }
  }

  test("/foo/bar") {
    assert(Path("/foo/bar").toList == List("foo", "bar"))
  }

  test("foo/bar") {
    assert(Path("foo/bar").toList == List("foo", "bar"))
  }

  test(":? extractor") {
    assert {
      (Path("/test.json") :? ParamMap()) match {
        case Root / "test.json" :? _ => true
        case _                       => false
      }
    }
  }

  test(":? extractor with ParamMatcher") {
    object A extends ParamMatcher("a")
    object B extends ParamMatcher("b")

    assert {
      (Path("/test.json") :? ParamMap("a" -> "1", "b" -> "2")) match {
        case Root / "test.json" :? A(a) => a == "1"
        case _                          => false
      }
    }
    assert {
      (Path("/test.json") :? ParamMap("a" -> "1", "b" -> "2")) match {
        case Root / "test.json" :? B(b) => b == "2"
        case _                          => false
      }
    }
    assert {
      (Path("/test.json") :? ParamMap("a" -> "1", "b" -> "2")) match {
        case Root / "test.json" :? (A(a) :& B(b)) => a == "1" && b == "2"
        case _                                    => false
      }
    }
    assert {
      (Path("/test.json") :? ParamMap("a" -> "1", "b" -> "2")) match {
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
      (Path("/test.json") :? ParamMap("i" -> "1", "l" -> "2147483648", "d" -> "1.3")) match {
        case Root / "test.json" :? (I(i) :& L(l) :& D(d)) => i == 1 && l == 2147483648L && d == 1.3D
        case _                                            => false
      }
    }
  }

  test("~ extractor on Path") {
    assert {
      Path("/foo.json") match {
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
      (Method.Get, Path("/test.json")) match {
        case Method.Get -> Root / "test.json" => true
        case _                                => false
      }
    }
  }

  test("Root extractor") {
    assert {
      Path("/") match {
        case Root => true
        case _    => false
      }
    }
  }

  test("Root extractor, no partial match") {
    assert {
      (Path("/test.json") match {
        case Root => true
        case _    => false
      }) == false
    }
  }

  test("Root extractor, empty path") {
    assert {
      Path("") match {
        case Root => true
        case _    => false
      }
    }
  }

  test("/ extractor") {
    assert {
      Path("/1/2/3/test.json") match {
        case Root / "1" / "2" / "3" / "test.json" => true
        case _                                    => false
      }
    }
  }

  test("Integer extractor") {
    assert {
      Path("/user/123") match {
        case Root / "user" / Integer(userId) => userId == 123
        case _                               => false
      }
    }
  }

  test("Integer extractor, negative int")  {
    assert {
      Path("/user/-123") match {
        case Root / "user" / Integer(userId) => userId == -123
        case _                               => false
      }
    }
  }

  test("Integer extractor, invalid int") {
    assert {
      (Path("/user/invalid") match {
        case Root / "user" / Integer(userId) => true
        case _                               => false
      }) == false
    }
  }

  test("Integer extractor, number format error") {
    assert {
      (Path("/user/2147483648") match {
        case Root / "user" / Integer(userId) => true
        case _                               => false
      }) == false
    }
  }

  test("Long extractor") {
    assert {
      Path("/user/123") match {
        case Root / "user" / Long(userId) => userId == 123
        case _                            => false
      }
    }
  }

  test("Long extractor, invalid int") {
    assert {
      (Path("/user/invalid") match {
        case Root / "user" / Long(userId) => true
        case _                            => false
      }) == false
    }
  }

  test("Long extractor, number format error") {
    assert {
      (Path("/user/9223372036854775808") match {
        case Root / "user" / Long(userId) => true
        case _                            => false
      }) == false
    }
  }
}
