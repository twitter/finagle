package com.twitter.finagle.stats

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class CumulativeGaugeSpec extends SpecificationWithJUnit with Mockito {
  class TestGauge extends CumulativeGauge {
    def register() {}
    def deregister() {}
  }

  "an empty CumulativeGauge" should {
    val gauge = spy(new TestGauge)
    there was no(gauge).register()

    "register on the first gauge added" in {
      gauge.addGauge { 0.0f }
      there was one(gauge).register()
    }
  }

  "a CumulativeGauge with size = 1" should {
    val gauge = spy(new TestGauge)
    var added = gauge.addGauge { 1.0f }
    there was no(gauge).deregister()

    "deregister when all gauges are removed" in {
      added.remove()
      there was one(gauge).deregister()
    }

    "not deregister after a System.gc when there are still valid references to the gauge" in {
      System.gc()

      // We have to incite some action for the weakref GC to take place.
      gauge.getValue must be_==(1.0f)
      there was no(gauge).deregister()
    }

    "deregister after a System.gc when no references are held onto" in {
      added = null
      System.gc()

      // We have to incite some action for the weakref GC to take place.
      gauge.getValue must be_==(0.0f)
      there was one(gauge).deregister()
    }
  }

  "a CumulativeGauge" should {
    val gauge = spy(new TestGauge)

    "sum values across all registered gauges" in {
      0 until 100 foreach { _ => gauge.addGauge { 10.0f } }
      gauge.getValue must be_==(10.0f * 100)
    }

    "discount gauges once removed" in {
      val underlying = 0 until 100 map { _ => gauge.addGauge { 10.0f } }
      gauge.getValue must be_==(10.0f * 100)
      underlying(0).remove()
      gauge.getValue must be_==(10.0f * 99)
      underlying(1).remove()
      gauge.getValue must be_==(10.0f * 98)
    }
  }
}
