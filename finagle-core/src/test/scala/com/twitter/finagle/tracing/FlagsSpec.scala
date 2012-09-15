package com.twitter.finagle.tracing

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class FlagsSpec extends SpecificationWithJUnit with Mockito {
  "Flags" should {
    "set flag and return it" in {
      val flags = Flags()
      flags.isFlagSet(Flags.Debug) mustEqual false

      val changed = flags.setFlag(Flags.Debug)
      changed.isFlagSet(Flags.Debug) mustEqual true
      changed.toLong mustEqual Flags.Debug

      flags.isFlagSet(Flags.Debug) mustEqual false
    }

    "set multiple flag and return it" in {
      val flags = Flags()
      flags.isFlagSet(1L) mustEqual false
      flags.isFlagSet(2L) mustEqual false

      val changed = flags.setFlags(Seq(1L, 2L))
      changed.isFlagSet(1L) mustEqual true
      changed.isFlagSet(2L) mustEqual true
      changed.toLong mustEqual 3L

      flags.isFlagSet(1L) mustEqual false
      flags.isFlagSet(2L) mustEqual false
    }
  }
}
