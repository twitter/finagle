package com.twitter.finagle.mux.lease.exp

import com.twitter.conversions.storage.intToStorageUnitableWholeNumber
import com.twitter.util.StorageUnit
import org.mockito.Mockito.{when, verify}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class MemorySpaceTest extends FunSuite with MockitoSugar {
  test("MemorySpace#left should find the number of bytes left before we hit minDiscount") {
    val nfo = mock[JvmInfo]
    when(nfo.remaining()).thenReturn(10.megabytes)
    val range = StorageUnit.zero
    val minDiscount = 5.megabytes
    val maxDiscount = StorageUnit.zero
    val rSnooper = mock[RequestSnooper]
    val space = new MemorySpace(nfo, minDiscount, maxDiscount, rSnooper)
    assert(space.left == 5.megabytes)
    verify(nfo).remaining()
  }

  test("MemorySpace should be able to compute a discount correctly") {
    val nfo = mock[JvmInfo]
    val minDiscount = 5.megabytes
    val maxDiscount = 10.megabytes
    val rSnooper = mock[RequestSnooper]
    when(rSnooper.handleBytes()).thenReturn(2.megabytes)
    val rnd = mock[GenerationalRandom]
    when(rnd.apply()).thenReturn(107.megabytes.inBytes.toInt)
    val space =
      new MemorySpace(nfo, minDiscount, maxDiscount, rSnooper, NullLogsReceiver, rnd)
    assert(space.discount() == 7.megabytes)
    verify(rnd).apply()
    verify(rSnooper).handleBytes()
  }

  test("MemorySpace should be able to default to a max") {
    val nfo = mock[JvmInfo]
    val minDiscount = 5.megabytes
    val maxDiscount = 8.megabytes
    val rSnooper = mock[RequestSnooper]
    when(rSnooper.handleBytes()).thenReturn(9.megabytes)
    val rnd = mock[GenerationalRandom]
    val space =
      new MemorySpace(nfo, minDiscount, maxDiscount, rSnooper, NullLogsReceiver, rnd)
    assert(space.discount() == 8.megabytes)
  }
}
