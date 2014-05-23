package com.twitter.finagle.stats

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{times, verify}
import org.mockito.Mockito

@RunWith(classOf[JUnitRunner])
class CumulativeGaugeTest extends FunSuite with MockitoSugar {
  class TestGauge extends CumulativeGauge {
    def register() {}
    def deregister() {}
  }

  test("an empty CumulativeGauge should register on the first gauge added") {
    val gauge = Mockito.spy(new TestGauge)
    verify(gauge, times(0)).register()

    gauge.addGauge { 0.0f }
    verify(gauge).register()
  }

  test("a CumulativeGauge with size = 1 should deregister when all gauges are removed") {
    val gauge = Mockito.spy(new TestGauge)
    var added = gauge.addGauge { 1.0f }
    verify(gauge, times(0)).deregister()

    added.remove()
    verify(gauge).deregister()
  }

  test("a CumulativeGauge with size = 1 should not deregister after a System.gc when there are still valid references to the gauge") {
    val gauge = Mockito.spy(new TestGauge)
    var added = gauge.addGauge { 1.0f }
    verify(gauge, times(0)).deregister()

    System.gc()

    // We have to incite some action for the weakref GC to take place.
    assert(gauge.getValue === 1.0f)
    verify(gauge, times(0)).deregister()
  }

  test("a CumulativeGauge with size = 1 should deregister after a System.gc when no references are held onto") {
    val gauge = Mockito.spy(new TestGauge)
    var added = gauge.addGauge { 1.0f }
    verify(gauge, times(0)).deregister()

    added = null
    System.gc()

    // We have to incite some action for the weakref GC to take place.
    assert(gauge.getValue === 0.0f)
    verify(gauge).deregister()
  }

  test("a CumulativeGauge should sum values across all registered gauges") {
    val gauge = Mockito.spy(new TestGauge)

    0 until 100 foreach { _ => gauge.addGauge { 10.0f } }
    assert(gauge.getValue === (10.0f * 100))
  }

  test("a CumulativeGauge should discount gauges once removed") {
    val gauge = Mockito.spy(new TestGauge)

    val underlying = 0 until 100 map { _ => gauge.addGauge { 10.0f } }
    assert(gauge.getValue === (10.0f * 100))
    underlying(0).remove()
    assert(gauge.getValue === (10.0f * 99))
    underlying(1).remove()
    assert(gauge.getValue === (10.0f * 98))
  }

}
