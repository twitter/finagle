package com.twitter.finagle.toggle

import com.twitter.util.{Return, Throw, Try}
import org.junit.runner.RunWith
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class JsonToggleMapTest extends FunSuite
  with GeneratorDrivenPropertyChecks {

  import JsonToggleMap.{DescriptionIgnored, DescriptionRequired}

  private def assertParseFails(input: String): Unit = {
    assertParseFails(input, DescriptionIgnored)
    assertParseFails(input, DescriptionRequired)
  }

  private def assertParseFails(
    input: String,
    descriptionMode: JsonToggleMap.DescriptionMode
  ): Unit =
    JsonToggleMap.parse(input, descriptionMode) match {
      case Return(_) => fail(s"Parsing should not succeed for $input")
      case Throw(_) => // expected
    }

  test("parse invalid JSON string with no toggles") {
    assertParseFails("{ }")
  }

  test("parse invalid JSON string with no id") {
    assertParseFails("""
      |{"toggles": [
      |    { "description": "Dude, where's my id?",
      |      "fraction": 0.0
      |    }
      |  ]
      |}""".stripMargin)
  }

  test("parse invalid JSON string with duplicate ids") {
    assertParseFails("""
      |{"toggles": [
      |    { "id": "com.twitter.duplicate",
      |      "description": "cannot have duplicate ids even if other fields differ",
      |      "fraction": 0.0
      |    },
      |    { "id": "com.twitter.duplicate",
      |      "description": "this is a duplicate",
      |      "fraction": 1.0
      |    }
      |  ]
      |}""".stripMargin)
  }

  test("parse invalid JSON string with empty description") {
    assertParseFails("""
      |{"toggles": [
      |    { "id": "com.twitter.EmptyDescription",
      |      "description": "    ",
      |      "fraction": 0.0
      |    }
      |  ]
      |}""".stripMargin,
      DescriptionRequired)
  }

  private val jsonWithNoDescription = """
      |{"toggles": [
      |    { "id": "com.twitter.NoDescription",
      |      "fraction": 0.0
      |    }
      |  ]
      |}""".stripMargin

  test("parse JSON string with no description and is required") {
    assertParseFails(jsonWithNoDescription, DescriptionRequired)
  }

  test("parse JSON string with no description and is ignored") {
    JsonToggleMap.parse(jsonWithNoDescription, DescriptionIgnored) match {
      case Throw(t) =>
        fail(t)
      case Return(tm) =>
        assert(tm.iterator.size == 1)
    }
  }

  test("parse invalid JSON string with invalid fraction") {
    assertParseFails("""
      |{"toggles": [
      |    { "id": "com.twitter.BadFraction",
      |      "description": "fractions should be 0-1",
      |      "fraction": 1.1
      |    }
      |  ]
      |}""".stripMargin)
  }

  test("parse invalid JSON string with no fraction") {
    assertParseFails("""
      |{"toggles": [
      |    { "id": "com.twitter.NoFraction",
      |      "description": "fractions must be present"
      |    }
      |  ]
      |}""".stripMargin)
  }

  // NOTE: this input should match what's in the resources file for
  // com/twitter/toggles/com.twitter.finagle.toggle.tests.Valid.json
  private val validInput = """
      |{
      |  "toggles": [
      |    {
      |      "id": "com.twitter.off",
      |      "description": "Always disabled, yo.",
      |      "fraction": 0.0
      |    },
      |    {
      |      "id": "com.twitter.on",
      |      "description": "Always enabled, dawg.",
      |      "fraction": 1.0,
      |      "comment": "Look, I'm on!"
      |    }
      |  ]
      |}""".stripMargin

  private def validateParsedJson(toggleMap: Try[ToggleMap]): Unit = {
    toggleMap match {
      case Throw(t) =>
        fail(t)
      case Return(map) =>
        assert(map.iterator.size == 2)
        assert(map.iterator.exists(_.id == "com.twitter.off"))
        assert(map.iterator.exists(_.id == "com.twitter.on"))

        val on = map("com.twitter.on")
        val off = map("com.twitter.off")
        val doestExist = map("com.twitter.lolcat")
        forAll(arbitrary[Int]) { i =>
          assert(on.isDefinedAt(i))
          assert(on(i))
          assert(off.isDefinedAt(i))
          assert(!off(i))
          assert(!doestExist.isDefinedAt(i))
        }
    }
  }

  test("parse valid JSON String") {
    validateParsedJson(JsonToggleMap.parse(validInput, DescriptionIgnored))
    validateParsedJson(JsonToggleMap.parse(validInput, DescriptionRequired))
  }

  test("parse valid JSON String with empty toggles") {
    val in = """
        |{
        |  "toggles": [ ]
        |}""".stripMargin
    JsonToggleMap.parse(in, DescriptionRequired) match {
      case Throw(t) =>
        fail(t)
      case Return(map) =>
        assert(0 == map.iterator.size)
    }
  }

  test("parse valid JSON resource file") {
    val rscs = getClass.getClassLoader.getResources(
      "com/twitter/toggles/configs/com.twitter.finagle.toggle.tests.Valid.json"
    ).asScala.toSeq

    assert(1 == rscs.size)
    validateParsedJson(JsonToggleMap.parse(rscs.head, DescriptionIgnored))
    validateParsedJson(JsonToggleMap.parse(rscs.head, DescriptionRequired))
  }

  test("parse invalid JSON resource file") {
    // this json file is missing an "id" on a toggle definition and therefore
    // should fail to parse.
    val rscs = getClass.getClassLoader.getResources(
      "com/twitter/toggles/configs/com.twitter.finagle.toggle.tests.Invalid.json"
    ).asScala.toSeq

    assert(1 == rscs.size)
    JsonToggleMap.parse(rscs.head, DescriptionIgnored) match {
      case Return(_) => fail(s"Parsing should not succeed")
      case Throw(_) => // expected
    }
  }

}
