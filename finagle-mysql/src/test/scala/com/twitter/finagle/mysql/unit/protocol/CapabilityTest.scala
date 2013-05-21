package com.twitter.finagle.exp.mysql.protocol

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class CapabilityTest extends FunSuite {
  val c = Capability(Capability.LongPassword,
                   Capability.SSL,
                   Capability.Transactions,
                   Capability.MultiResults)

  test("contain capability") {
    expectResult(true) { c.has(Capability.SSL) }
    expectResult(false) { c.has(Capability.Compress) }
  }

  test("contain all capabilities") {
    expectResult(false) { c.hasAll(Capability.LongPassword, Capability.NoSchema) }
    expectResult(true) {
      c.hasAll(
        Capability.LongPassword,
        Capability.SSL,
        Capability.Transactions,
        Capability.MultiResults
      )
    }
  }

  test("subtract capability") {
    val c2 = c - Capability.SSL
    expectResult(false) { c2.has(Capability.SSL) }
  }

  test("add capability") {
    val c2 = c + Capability.LocalFiles + Capability.Compress
    expectResult(true) {
      c2.hasAll(
        Capability.LocalFiles,
        Capability.Compress
      )
    }
  }
}
