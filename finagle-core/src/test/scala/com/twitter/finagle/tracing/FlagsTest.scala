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

  test("set and get sampleRate") {
    val flags = Flags()
    val sampleRate = Float.MinPositiveValue
    val changed = flags.setSampleRate(sampleRate)
    assert(changed.getSampleRate() == sampleRate)
  }

  test("setSampleRate throws exception on negative sampleRate") {
    intercept[IllegalArgumentException] {
      Flags().setSampleRate(-0.1f)
    }
  }

  test("sampleRate flag doesn't clobber the others") {
    val flags = Flags()
    val sampleRate = Float.MinPositiveValue
    assert(!flags.isFlagSet(Flags.Debug))
    assert(!flags.isFlagSet(Flags.Sampled))
    assert(!flags.isFlagSet(Flags.SamplingKnown))

    val changed = flags.setFlags(Seq(Flags.Debug, Flags.SamplingKnown, Flags.Sampled))
    assert(changed.isFlagSet(Flags.Debug))
    assert(changed.isFlagSet(Flags.Sampled))
    assert(changed.isFlagSet(Flags.SamplingKnown))

    val changedWithSR = changed.setSampleRate(sampleRate)
    assert(changedWithSR.isFlagSet(Flags.Debug))
    assert(changedWithSR.isFlagSet(Flags.Sampled))
    assert(changedWithSR.isFlagSet(Flags.SamplingKnown))

    assert(changedWithSR.getSampleRate() == sampleRate)
  }
}
