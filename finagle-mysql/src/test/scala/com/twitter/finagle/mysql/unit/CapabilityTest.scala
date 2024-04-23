package com.twitter.finagle.mysql

import org.scalatest.funsuite.AnyFunSuite

class CapabilityTest extends AnyFunSuite {
  val c = Capability(
    Capability.LongPassword,
    Capability.SSL,
    Capability.Transactions,
    Capability.MultiResults
  )

  test("contain capability") {
    assert(c.has(Capability.SSL))
    assert(!c.has(Capability.Compress))
  }

  test("contain all capabilities") {
    assert(!c.hasAll(Capability.LongPassword, Capability.NoSchema))
    assert(
      c.hasAll(
        Capability.LongPassword,
        Capability.SSL,
        Capability.Transactions,
        Capability.MultiResults))
  }

  test("subtract capability") {
    val c2 = c - Capability.SSL
    assert(!c2.has(Capability.SSL))
  }

  test("add capability") {
    val c2 = c + Capability.LocalFiles + Capability.Compress
    assert(c2.hasAll(Capability.LocalFiles, Capability.Compress))
  }

  test("set capability based on condition") {
    val withSsl = c.set(true, Capability.SSL)
    assert(withSsl.has(Capability.SSL))
    val withoutSsl = c.set(false, Capability.SSL)
    assert(!withoutSsl.has(Capability.SSL))

    val withFoundRows = c.set(true, Capability.FoundRows)
    assert(withFoundRows.has(Capability.FoundRows))
    val withoutFoundRows = c.set(false, Capability.FoundRows)
    assert(!withoutFoundRows.has(Capability.FoundRows))

    val withInteractive = c.set(true, Capability.Interactive)
    assert(withInteractive.has(Capability.Interactive))
    val withoutInteractive = c.set(false, Capability.Interactive)
    assert(!withoutInteractive.has(Capability.Interactive))
  }

  test("toString lists capabilities") {
    val capabilities = Capability(Capability.SSL + Capability.PluginAuth)
    val expected = "Capability(526336: CLIENT_PLUGIN_AUTH, CLIENT_SSL)"
    assert(capabilities.toString == expected)
  }
}
