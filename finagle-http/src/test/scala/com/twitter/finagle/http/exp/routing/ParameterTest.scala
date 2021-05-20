package com.twitter.finagle.http.exp.routing

import org.scalatest.funsuite.AnyFunSuite

class ParameterTest extends AnyFunSuite {

  test("StringParam#parse") {
    val param = StringParam("str")

    assert(param.parse("abc") == Some(StringValue("abc")))

    val parsed = param.parse("xyz").get
    assert(parsed.value == "xyz")
  }

  test("BooleanParam#parse") {
    val param = BooleanParam("bool")

    assert(param.parse("true") == Some(BooleanValue("true", true)))
    assert(param.parse("True") == Some(BooleanValue("True", true)))
    assert(param.parse("TRUE") == Some(BooleanValue("TRUE", true)))

    assert(param.parse("false") == Some(BooleanValue("false", false)))
    assert(param.parse("False") == Some(BooleanValue("False", false)))
    assert(param.parse("FALSE") == Some(BooleanValue("FALSE", false)))

    assert(param.parse("Pineapple") == None)

    // not currently supported by the OpenAPI spec
    assert(param.parse("t") == None)
    assert(param.parse("f") == None)
    assert(param.parse("1") == None)
    assert(param.parse("0") == None)
  }

  test("IntParam#parse") {
    val param = IntParam("int")

    assert(param.parse("123") == Some(IntValue("123", 123)))
    assert(param.parse("1.23") == None)
    assert(param.parse("0") == Some(IntValue("0", 0)))
    assert(param.parse("-321") == Some(IntValue("-321", -321)))
    assert(param.parse("-3.21") == None)
    assert(param.parse("Pineapple") == None)
    assert(param.parse(Long.MaxValue.toString) == None)
    assert(param.parse(Long.MinValue.toString) == None)
  }

  test("LongParam#parse") {
    val param = LongParam("long")

    assert(param.parse("123") == Some(LongValue("123", 123)))
    assert(param.parse("1.23") == None)
    assert(param.parse("0") == Some(LongValue("0", 0)))
    assert(param.parse("-321") == Some(LongValue("-321", -321)))
    assert(param.parse("-3.21") == None)
    assert(param.parse("Pineapple") == None)
    assert(
      param.parse(Long.MaxValue.toString) == Some(LongValue(Long.MaxValue.toString, Long.MaxValue)))
    assert(
      param.parse(Long.MinValue.toString) == Some(LongValue(Long.MinValue.toString, Long.MinValue)))
  }

  test("FloatParam#parse") {
    val param = FloatParam("float")

    assert(param.parse("123") == Some(FloatValue("123", 123)))
    assert(param.parse("1.23") == Some(FloatValue("1.23", 1.23f)))
    assert(param.parse("0") == Some(FloatValue("0", 0f)))
    assert(param.parse("0.000") == Some(FloatValue("0.000", 0f)))
    assert(param.parse("-321") == Some(FloatValue("-321", -321f)))
    assert(param.parse("-3.21") == Some(FloatValue("-3.21", -3.21f)))
    assert(param.parse("Pineapple") == None)

    assert(
      param.parse(Float.MaxValue.toString) == Some(
        FloatValue(Float.MaxValue.toString, Float.MaxValue)))
    assert(
      param.parse(Float.MinValue.toString) == Some(
        FloatValue(Float.MinValue.toString, Float.MinValue)))

    // Float is weird and will parse Double and completely botch the results
    assert(param.parse(Double.MaxValue.toString).isDefined)
    assert(
      param.parse(Double.MaxValue.toString) != Some(
        FloatValue(Float.MaxValue.toString, Float.MaxValue)))
  }

  test("DoubleParam#parse") {
    val param = DoubleParam("double")

    assert(param.parse("123") == Some(DoubleValue("123", 123)))
    assert(param.parse("1.23") == Some(DoubleValue("1.23", 1.23d)))
    assert(param.parse("0") == Some(DoubleValue("0", 0)))
    assert(param.parse("0.000") == Some(DoubleValue("0.000", 0)))
    assert(param.parse("-321") == Some(DoubleValue("-321", -321d)))
    assert(param.parse("-3.21") == Some(DoubleValue("-3.21", -3.21d)))
    assert(param.parse("Pineapple") == None)
    assert(
      param.parse(Double.MaxValue.toString) == Some(
        DoubleValue(Double.MaxValue.toString, Double.MaxValue)))
    assert(
      param.parse(Double.MinValue.toString) == Some(
        DoubleValue(Double.MinValue.toString, Double.MinValue)))
  }

}
