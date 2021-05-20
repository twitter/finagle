package com.twitter.finagle.http.exp.routing

import org.scalatest.funsuite.AnyFunSuite

class PathTest extends AnyFunSuite {

  test("toString") {
    assert(Path(Seq(Segment.Slash)).toString == "/")
    assert(Path(Seq(Segment.Slash, Segment.Constant("users"))).toString == "/users")

    val path = Path(
      Seq(
        Segment.Slash,
        Segment.Constant("users"),
        Segment.Slash,
        Segment.Parameterized(IntParam("id"))))
    assert(path.toString == "/users/{id}")

    val path2 = Path(
      Seq(
        Segment.Slash,
        Segment.Constant("users"),
        Segment.Slash,
        Segment.Parameterized(StringParam("string")),
        Segment.Slash,
        Segment.Parameterized(IntParam("int")),
        Segment.Slash,
        Segment.Parameterized(LongParam("long")),
        Segment.Slash,
        Segment.Parameterized(BooleanParam("boolean")),
        Segment.Slash,
        Segment.Parameterized(DoubleParam("double")),
        Segment.Slash,
        Segment.Parameterized(FloatParam("float"))
      ))

    assert(path2.toString == "/users/{string}/{int}/{long}/{boolean}/{double}/{float}")

    val path3 =
      Path(Seq(Segment.Slash, Segment.Parameterized(StringParam("name")), Segment.Constant(".jpg")))
    assert(path3.toString == "/{name}.jpg")

    val path4 = Path(
      Seq(
        Segment.Slash,
        Segment.Constant("file."),
        Segment.Parameterized(StringParam("extension"))))
    assert(path4.toString == "/file.{extension}")
  }
}
