package com.twitter.finagle.toggle

import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import org.scalacheck.Arbitrary.arbitrary
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

class JsonToggleMapTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  import JsonToggleMap.DescriptionIgnored
  import JsonToggleMap.DescriptionRequired
  import JsonToggleMap.FailParsingOnDuplicateId
  import JsonToggleMap.KeepFirstOnDuplicateId

  private def assertParseFails(input: String): Unit = {
    assertParseFails(input, DescriptionIgnored)
    assertParseFails(input, DescriptionRequired)
  }

  private def assertParseFails(
    input: String,
    descriptionMode: JsonToggleMap.DescriptionMode,
    duplicateHandling: JsonToggleMap.DuplicateHandling = JsonToggleMap.FailParsingOnDuplicateId
  ): Unit =
    JsonToggleMap.parse(input, descriptionMode, duplicateHandling) match {
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

  val jsonWithDuplicates =
    """
      |{
      |"toggles": [
      |  {
      |    "id": "com.twitter.duplicate",
      |    "description": "cannot have duplicate ids even if other fields differ",
      |    "fraction": 1.0
      |  },
      |  {
      |    "id": "com.twitter.duplicate",
      |    "description": "this is a duplicate",
      |    "fraction": 0.1
      |  }
      |]
      |}""".stripMargin

  test("parse invalid JSON string with duplicate ids") {
    assertParseFails(jsonWithDuplicates)
  }

  test("parse JSON string with duplicate ids and keep first when KeepFirstOnDuplicateId is set") {
    JsonToggleMap.parse(jsonWithDuplicates, DescriptionIgnored, KeepFirstOnDuplicateId) match {
      case Throw(t) =>
        fail(t)
      case Return(tm) =>
        assert(tm.iterator.size == 1)
        assert(tm.apply("com.twitter.duplicate").isDefined)
        assert(tm.apply("com.twitter.duplicate").isEnabled(1))
    }
  }

  test("parse invalid JSON string with empty description") {
    assertParseFails(
      """
      |{"toggles": [
      |    { "id": "com.twitter.EmptyDescription",
      |      "description": "    ",
      |      "fraction": 0.0
      |    }
      |  ]
      |}""".stripMargin,
      DescriptionRequired
    )
  }

  private val jsonWithNoDescription = """
      |{"toggles": [
      |    { "id": "com.twitter.NoDescription",
      |      "fraction": 0.0
      |    }
      |  ]
      |}""".stripMargin

  test("parse JSON string with no description and is required") {
    assertParseFails(jsonWithNoDescription, DescriptionRequired, FailParsingOnDuplicateId)
  }

  test("parse JSON string with no description and is ignored") {
    JsonToggleMap.parse(jsonWithNoDescription, DescriptionIgnored, FailParsingOnDuplicateId) match {
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
          assert(on.isDefined)
          assert(on(i))
          assert(off.isDefined)
          assert(!off(i))
          assert(!doestExist.isDefined)
        }
    }
  }

  test("parse valid JSON String") {
    validateParsedJson(
      JsonToggleMap.parse(validInput, DescriptionIgnored, FailParsingOnDuplicateId))
    validateParsedJson(
      JsonToggleMap.parse(validInput, DescriptionRequired, FailParsingOnDuplicateId))
  }

  test("parse valid JSON String with empty toggles") {
    val in = """
        |{
        |  "toggles": [ ]
        |}""".stripMargin
    JsonToggleMap.parse(in, DescriptionRequired, FailParsingOnDuplicateId) match {
      case Throw(t) =>
        fail(t)
      case Return(map) =>
        assert(0 == map.iterator.size)
    }
  }

  test("parse valid JSON resource file") {
    val rscs = getClass.getClassLoader
      .getResources(
        "com/twitter/toggles/configs/com.twitter.finagle.toggle.tests.Valid.json"
      )
      .asScala
      .toSeq

    assert(1 == rscs.size)
    validateParsedJson(JsonToggleMap.parse(rscs.head, DescriptionIgnored, FailParsingOnDuplicateId))
    validateParsedJson(
      JsonToggleMap.parse(rscs.head, DescriptionRequired, FailParsingOnDuplicateId))
  }

  test("parse invalid JSON resource file") {
    // this json file is missing an "id" on a toggle definition and therefore
    // should fail to parse.
    val rscs = getClass.getClassLoader
      .getResources(
        "com/twitter/toggles/configs/com.twitter.finagle.toggle.tests.Invalid.json"
      )
      .asScala
      .toSeq

    assert(1 == rscs.size)
    JsonToggleMap.parse(rscs.head, DescriptionIgnored, FailParsingOnDuplicateId) match {
      case Return(_) => fail(s"Parsing should not succeed")
      case Throw(_) => // expected
    }
  }

}
