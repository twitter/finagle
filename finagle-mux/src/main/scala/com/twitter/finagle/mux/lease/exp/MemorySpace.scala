package com.twitter.finagle.mux.lease.exp

import com.twitter.conversions.storage.longToStorageUnitableWholeNumber
import com.twitter.util.StorageUnit

/**
 * MemorySpace is a representation of the JVM Eden space for the Par New garbage
 * collector that is aware of the semantics of the GC Avoidance implementation.
 * Namely, it keeps track of the *minimum discount*, which represents the point
 * at which we will force a gc regardless of whether all clients have finished
 * draining, and other internal abstractions.
 */
private[lease] class MemorySpace(
  info: JvmInfo,
  discountRange: StorageUnit,
  val minDiscount: StorageUnit,
  maxDiscount: StorageUnit,
  rSnooper: RequestSnooper,
  lr: LogsReceiver,
  rnd: GenerationalRandom
) {
  def this(
    info: JvmInfo,
    discountRange: StorageUnit,
    minDiscount: StorageUnit,
    maxDiscount: StorageUnit,
    rSnooper: RequestSnooper,
    lr: LogsReceiver
  ) = this(info, discountRange, minDiscount, maxDiscount, rSnooper, lr, new GenerationalRandom(info))

  def this(
    info: JvmInfo,
    discountRange: StorageUnit,
    minDiscount: StorageUnit,
    maxDiscount: StorageUnit,
    rSnooper: RequestSnooper
  ) = this(info, discountRange, minDiscount, maxDiscount, rSnooper, NullLogsReceiver)

  /**
   * The target for lease expiration.  It should contain jitter so that not
   * every service in a cluster expires simultaneously if they have very similar
   * garbage collection properties, but should also be consistent across a given
   * generation.
   */
  def discount(): StorageUnit = {
    val handleBytes: StorageUnit = rSnooper.handleBytes()
    lr.record("discountHandleBytes", handleBytes.toString)

    //choose a random number of bytes between 0 and discountRange
    val discountWin = (rnd() % discountRange.inBytes).bytes
    lr.record("discountWin", discountWin.inBytes.toString)

    // take the min of jitter within a range + handlebytes, and maxDiscount
    val discountTotal = (handleBytes + discountWin) min maxDiscount
    lr.record("discountTotal", discountTotal.toString)

    discountTotal
  }

  /**
   * The difference between the bytes remaining in the Eden space and the minimum
   * discount.
   */
  def left: StorageUnit = info.remaining() - minDiscount

  override def toString(): String =
    "MemorySpace(left=" + left + ", discount=" + discount() + ", info=" + info + ")"
}
