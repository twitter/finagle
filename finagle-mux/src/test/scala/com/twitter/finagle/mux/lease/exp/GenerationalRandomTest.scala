package com.twitter.finagle.mux.lease.exp

import org.mockito.Mockito.{when, verify, times}
import org.scalatestplus.mockito.MockitoSugar
import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite

class GenerationalRandomTest extends AnyFunSuite with MockitoSugar {
  test("GenerationalRandom stays the same intragenerationally") {
    val nfo = mock[JvmInfo]
    when(nfo.generation()).thenReturn(0)
    val rnd = new Random(12334)
    val gen = new GenerationalRandom(nfo, rnd)
    verify(nfo).generation()

    val x = gen()
    verify(nfo, times(2)).generation()

    assert(gen() == x)
    verify(nfo, times(3)).generation()
  }

  test("GenerationalRandom changes intergenerationally") {
    val nfo = mock[JvmInfo]
    when(nfo.generation()).thenReturn(0)
    val rnd = new Random(12334)
    val gen = new GenerationalRandom(nfo, rnd)
    verify(nfo).generation()

    val x = gen()
    verify(nfo, times(2)).generation()

    assert(gen() == x)
    verify(nfo, times(3)).generation()

    when(nfo.generation()).thenReturn(1)

    assert(gen() != x)
    verify(nfo, times(5)).generation()
  }
}
