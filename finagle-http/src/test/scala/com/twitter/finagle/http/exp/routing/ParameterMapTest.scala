package com.twitter.finagle.http.exp.routing

import org.scalatest.funsuite.AnyFunSuite

class ParameterMapTest extends AnyFunSuite {

  private[this] val map = Map(
    "string" -> StringValue("hello"),
    "int" -> IntValue("123", 123),
    "long" -> LongValue("456", 456L),
    "boolean" -> BooleanValue("true", true),
    "double" -> DoubleValue("1.234", 1.234d),
    "float" -> FloatValue("5.678", 5.678f)
  )

  private[this] val parameters: ParameterMap = new MapParameterMap(map)

  test("EmptyParameterMap") {
    val map: ParameterMap = EmptyParameterMap
    assert(map.get("id") == None)
    assert(map.getInt("id") == None)
    assert(map.getBoolean("id") == None)
    assert(map.getLong("id") == None)
    assert(map.getFloat("id") == None)
    assert(map.getDouble("id") == None)
  }

  test("getParam") {
    map.keys.foreach { name =>
      assert(parameters.get(name).isDefined)
      assert(EmptyParameterMap.get(name).isDefined == false)
    }
  }

  test("getParamClass") {
    assert(parameters.getParamClass("string") == Some(classOf[String]))
    assert(parameters.getParamClass("int") == Some(classOf[Int]))
    assert(parameters.getParamClass("long") == Some(classOf[Long]))
    assert(parameters.getParamClass("boolean") == Some(classOf[Boolean]))
    assert(parameters.getParamClass("float") == Some(classOf[Float]))
    assert(parameters.getParamClass("double") == Some(classOf[Double]))
    assert(parameters.getParamClass("pineapple") == None)
  }

  test("isDefinedAt") {
    map.keys.foreach { name =>
      assert(parameters.isDefinedAt(name))
      assert(EmptyParameterMap.isDefinedAt(name) == false)
    }
  }

  test("get") {
    map.keys.foreach { name =>
      assert(parameters.get(name).isDefined)
      assert(parameters.get(name).get == map(name).value)
    }
  }

  test("getInt") {
    testTypedParam("int", (name, path) => path.getInt(name))
  }

  test("getLong") {
    testTypedParam("long", (name, path) => path.getLong(name))
  }

  test("getBoolean") {
    testTypedParam("boolean", (name, path) => path.getBoolean(name))
  }

  test("getFloat") {
    testTypedParam("float", (name, path) => path.getFloat(name))
  }

  test("getDouble") {
    testTypedParam("double", (name, path) => path.getDouble(name))
  }

  /**
   *
   * @param name The parameter name that we expect will contain a result of the expected type
   * @param getFromMap The function to retrieve the named parameter from the ParameterMap
   */
  private[this] def testTypedParam(
    name: String,
    getFromMap: (String, ParameterMap) => Option[_]
  ): Unit =
    map.keys.foreach { n =>
      // we should have a result of the expected type for our parameter name
      val typedResult = getFromMap(n, parameters)

      // we assert that the result is defined if we have the expected name for the expected type,
      // otherwise we expect that the name is NOT defined for a type that doesn't match.
      // for example "int" -> "IntValue" should only be defined for `path.getInt("int")`.
      // it should never be defined for other methods, ex: `path.getLong("int")` is not defined.
      assertResult(n == name)(typedResult.isDefined)
    }

}
