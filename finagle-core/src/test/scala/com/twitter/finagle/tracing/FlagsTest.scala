package com.twitter.finagle.tracing

import org.scalatest.funsuite.AnyFunSuite

class FlagsTest extends AnyFunSuite {
  test("set flag and return it") {
    val flags = Flags()
    assert(!flags.isFlagSet(Flags.Debug))

    val changed = flags.setFlag(Flags.Debug)
    assert(changed.isFlagSet(Flags.Debug))
    assert(changed.toLong == Flags.Debug)

    assert(!flags.isFlagSet(Flags.Debug))
  }

  test("set multiple flag and return it") {
    val flags = Flags()
    assert(!flags.isFlagSet(1L))
    assert(!flags.isFlagSet(2L))

    val changed = flags.setFlags(Seq(1L, 2L))
    assert(changed.isFlagSet(1L))
    assert(changed.isFlagSet(2L))
    assert(changed.toLong == 3L)

    assert(!flags.isFlagSet(1L))
    assert(!flags.isFlagSet(2L))
  }
}
