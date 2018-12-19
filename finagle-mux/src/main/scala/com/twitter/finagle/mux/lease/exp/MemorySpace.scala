package com.twitter.finagle.mux.lease.exp

import com.twitter.conversions.StorageUnitOps._
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
  val minDiscount: StorageUnit,
  maxDiscount: StorageUnit,
  rSnooper: RequestSnooper,
  lr: LogsReceiver,
  rnd: GenerationalRandom) {
  def this(
    info: JvmInfo,
    minDiscount: StorageUnit,
    maxDiscount: StorageUnit,
    rSnooper: RequestSnooper,
    lr: LogsReceiver
  ) = this(info, minDiscount, maxDiscount, rSnooper, lr, new GenerationalRandom(info))

  def this(
    info: JvmInfo,
    minDiscount: StorageUnit,
    maxDiscount: StorageUnit,
    rSnooper: RequestSnooper
  ) = this(info, minDiscount, maxDiscount, rSnooper, NullLogsReceiver)

  private[this] val printableZeroBytes = 0.bytes.toString

  /**
   * The target for lease expiration.  It should contain jitter so that not
   * every service in a cluster expires simultaneously if they have very similar
   * garbage collection properties, but should also be consistent across a given
   * generation.
   */
  def discount(): StorageUnit = {
    val handleBytes: StorageUnit = rSnooper.handleBytes()
    lr.record("discountHandleBytes", handleBytes.toString)

    if (handleBytes < maxDiscount) {
      val low = handleBytes max minDiscount

      // choose a random number of bytes between 0 and discountRange
      val discountWin = (rnd() % (maxDiscount - low).inBytes).bytes
      lr.record("discountWin", discountWin.toString)

      // take the min of jitter within a range + handlebytes, and maxDiscount
      val discountTotal = low + discountWin
      lr.record("discountTotal", discountTotal.toString)

      discountTotal
    } else {
      lr.record("discountWin", printableZeroBytes)
      lr.record("discountTotal", printableZeroBytes)

      maxDiscount
    }
  }

  /**
   * The difference between the bytes remaining in the Eden space and the minimum
   * discount.
   */
  def left: StorageUnit = info.remaining() - minDiscount

  override def toString(): String =
    "MemorySpace(left=" + left + ", discount=" + discount() + ", info=" + info + ")"
}
