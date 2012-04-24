package com.twitter.finagle.util

import org.specs.SpecificationWithJUnit

class ByteArraysSpec extends SpecificationWithJUnit {
  "ByteArrays" should {
    "concat" in {
      "add two arrays" in {
        val a = Array[Byte](0, 1, 2)
        val b = Array[Byte](3, 4)
        val c = ByteArrays.concat(a, b)
        c must haveSameElementsAs(Array[Byte](0, 1, 2, 3, 4))
      }

      "empty on the left" in {
        val a = Array[Byte]()
        val b = Array[Byte](3, 4)
        val c = ByteArrays.concat(a, b)
        c must haveSameElementsAs(Array[Byte](3, 4))
      }

      "empty on the right" in {
        val a = Array[Byte](0, 1, 2)
        val b = Array[Byte]()
        val c = ByteArrays.concat(a, b)
        c must haveSameElementsAs(Array[Byte](0, 1, 2))
      }

      "empty on both sides" in {
        val a = Array[Byte]()
        val b = Array[Byte]()
        val c = ByteArrays.concat(a, b)
        c must beEmpty
      }
    }
  }
}
