package com.twitter.finagle.http.exp.routing

import com.twitter.finagle.http.exp.routing.PathMatcher._
import org.scalatest.funsuite.AnyFunSuite

class PathMatcherTest extends AnyFunSuite {

  test("LinearPathMatcher#apply matches constant Path") {
    val path = Path(Seq(Segment.Slash, Segment.Constant("users")))
    assert(LinearPathMatcher(path, "/users") == ConstantPathMatch)
    assert(LinearPathMatcher(path, "///users") == ConstantPathMatch)
    assert(LinearPathMatcher(path, "/users/id") == NoPathMatch)
    assert(LinearPathMatcher(path, "/") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/") == NoPathMatch)
    assert(LinearPathMatcher(path, "users") == NoPathMatch)

    // case-sensitive check
    assert(LinearPathMatcher(path, "/Users") == NoPathMatch)
    assert(LinearPathMatcher(path, "///USERS") == NoPathMatch)
  }

  test("LinearPathMatcher#apply matches a Slash Path") {
    val path = Path(Seq(Segment.Slash))
    assert(LinearPathMatcher(path, "/") == ConstantPathMatch)
    assert(LinearPathMatcher(path, "///") == ConstantPathMatch)
    assert(LinearPathMatcher(path, "///////") == ConstantPathMatch)
    assert(LinearPathMatcher(path, "") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/") == NoPathMatch)
    assert(LinearPathMatcher(path, "users") == NoPathMatch)
  }

  test("LinearPathMatcher#apply matches dynamic Path with Int") {
    val path = Path(
      Seq(
        Segment.Slash,
        Segment.Constant("users"),
        Segment.Slash,
        Segment.Parameterized(IntParam("id"))))
    assert(
      LinearPathMatcher(path, "/users/123") == ParameterizedPathMatch(
        MapParameterMap(Map("id" -> IntValue("123", 123)))))
    assert(LinearPathMatcher(path, "/users/false") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/1.234") == NoPathMatch)
    assert(LinearPathMatcher(path, "///users") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/id") == NoPathMatch)
    assert(LinearPathMatcher(path, "/") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/") == NoPathMatch)
    assert(LinearPathMatcher(path, "users") == NoPathMatch)

    // case-sensitive check
    assert(LinearPathMatcher(path, "/Users/123") == NoPathMatch)
    assert(LinearPathMatcher(path, "/USERS/123") == NoPathMatch)
  }

  test("LinearPathMatcher#apply matches file extension pattern") {
    val path = Path(
      Seq(
        Segment.Slash,
        Segment.Constant("users"),
        Segment.Slash,
        Segment.Parameterized(IntParam("id")),
        Segment.Constant(".json")))
    assert(
      LinearPathMatcher(path, "/users/123.json") == ParameterizedPathMatch(
        MapParameterMap(Map("id" -> IntValue("123", 123)))))
    assert(LinearPathMatcher(path, "/users/123") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/false") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/1.234") == NoPathMatch)
    assert(LinearPathMatcher(path, "///users") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/id") == NoPathMatch)
    assert(LinearPathMatcher(path, "/") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/") == NoPathMatch)
    assert(LinearPathMatcher(path, "users") == NoPathMatch)

    // case-sensitive check
    assert(LinearPathMatcher(path, "/users/123.JSON") == NoPathMatch)
    assert(LinearPathMatcher(path, "/Users/123.json") == NoPathMatch)
    assert(LinearPathMatcher(path, "/USERS/123.json") == NoPathMatch)
  }

  test("LinearPathMatcher#apply matches multiple dynamic paths") {
    val path = Path(
      Seq(
        Segment.Slash,
        Segment.Parameterized(StringParam("name")),
        Segment.Slash,
        Segment.Parameterized(IntParam("id")),
        Segment.Constant(".json")))

    val matched: ParameterMap =
      LinearPathMatcher(path, "/abc/123.json") match {
        case ParameterizedPathMatch(params) => params
        case r => fail(s"unexpected result type: $r")
      }
    assert(matched.getInt("id") == Some(123))
    assert(matched.getString("name") == Some("abc"))
    assert(matched.getInt("name") == None)
    assert(matched.getString("id") == None)
    assert(LinearPathMatcher(path, "/users/123") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/false") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/1.234") == NoPathMatch)
    assert(LinearPathMatcher(path, "///users") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/id") == NoPathMatch)
    assert(LinearPathMatcher(path, "/") == NoPathMatch)
    assert(LinearPathMatcher(path, "/users/") == NoPathMatch)
    assert(LinearPathMatcher(path, "users") == NoPathMatch)

    // case-sensitive check
    assert(LinearPathMatcher(path, "/abc/123.JSON") == NoPathMatch)
  }

  test("PathMatcher#constant") {
    assert(PathMatcher.indexAfterRegionMatch("/users", 1, "users") == 6)
    assert(PathMatcher.indexAfterRegionMatch("/Users", 1, "users") == -1)
    assert(PathMatcher.indexAfterRegionMatch("/USERS", 1, "users") == -1)
    assert(PathMatcher.indexAfterRegionMatch("/users", 0, "users") == -1)
    assert(PathMatcher.indexAfterRegionMatch("/users/123.json", 10, ".json") == 15)
    assert(PathMatcher.indexAfterRegionMatch("/users/123", 6, ".json") == -1)
  }

  test("PathMatcher#slash") {
    assert(PathMatcher.indexAfterMatchingSlash("/users/123", 0) == 1)
    assert(PathMatcher.indexAfterMatchingSlash("/users/123", 6) == 7)
    assert(PathMatcher.indexAfterMatchingSlash("/users///123", 6) == 9)
    assert(PathMatcher.indexAfterMatchingSlash("/users123", 2) == -1)
  }

  test("PathMatcher#parameterized") {
    val start = Segment.Parameterized(IntParam("id"))
    assert(
      PathMatcher.parameterized("/users/123/abc", 7, start, Segment.Slash) == ParsedValue(
        11,
        IntValue("123", 123)))
    assert(
      PathMatcher.parameterized(
        "/users/123.json",
        7,
        start,
        Segment.Constant(".json")) == ParsedValue(15, IntValue("123", 123)))
    assert(
      PathMatcher
        .parameterized("/users/123.xml", 7, start, Segment.Constant(".json")) == UnableToParse)
  }

  test("PathMatcher#parameterizedEnd") {
    val segment = Segment.Parameterized(StringParam("end"))
    assert(
      PathMatcher.parameterizedEnd("/users/123-abc", 7, segment) == Some(StringValue("123-abc")))
    assert(PathMatcher.parameterizedEnd("/users/123/abc", 7, segment) == None)

    val intSegment = Segment.Parameterized(IntParam("end"))
    assert(PathMatcher.parameterizedEnd("/users/123", 7, intSegment) == Some(IntValue("123", 123)))
    assert(PathMatcher.parameterizedEnd("/users/123-abc", 7, intSegment) == None)
    assert(PathMatcher.parameterizedEnd("/users/123/abc", 7, intSegment) == None)
  }

  test("PathMatcher#segmentBoundary") {
    assert(PathMatcher.segmentBoundary("/abc/123", 2, Segment.Slash) == Boundary(4, 5))
    assert(PathMatcher.segmentBoundary("/abc///xyz", 2, Segment.Slash) == Boundary(4, 7))
    assert(PathMatcher.segmentBoundary("/abc/123/////567", 5, Segment.Slash) == Boundary(8, 13))
    assert(PathMatcher.segmentBoundary("/abc", 2, Segment.Slash) == NoBoundary)

    val const = Segment.Constant("-xyz-")
    assert(PathMatcher.segmentBoundary("/abc-xyz-123", 2, const) == Boundary(4, 9))
    assert(PathMatcher.segmentBoundary("/abc-123", 2, const) == NoBoundary)

    val json = Segment.Constant(".json")
    assert(PathMatcher.segmentBoundary("/abc.json", 2, json) == Boundary(4, 9))
    assert(PathMatcher.segmentBoundary("/abc-123", 2, json) == NoBoundary)
  }

}
