package com.twitter.finagle.mux.lease.exp

import java.lang.management.{GarbageCollectorMXBean, MemoryPoolMXBean, MemoryUsage}
import javax.management.ObjectName
import com.twitter.conversions.StorageUnitOps._
import com.twitter.util.StorageUnit

private[lease] trait MemoryPool {
  def snapshot(): MemoryPoolInfo
}

private[lease] class BeanMemoryPool(pool: MemoryPoolMXBean) extends MemoryPool {
  def snapshot(): MemoryPoolInfo = new MemoryUsageInfo(pool.getUsage())
}

private[lease] class FakeMemoryPool(original: MemoryPoolInfo) extends MemoryPool {
  @volatile private[this] var _snapshot: MemoryPoolInfo = original
  def setSnapshot(snap: MemoryPoolInfo): Unit = {
    _snapshot = snap
  }

  def snapshot() = _snapshot
}

private[lease] class FakeGarbageCollectorMXBean(
  @volatile var getCollectionCount: Long,
  @volatile var getCollectionTime: Long)
    extends GarbageCollectorMXBean {
  private[this] def ??? = throw new UnsupportedOperationException("not supported")

  def getMemoryPoolNames(): Array[String] = ???
  def isValid = true
  def getName: String = ???
  def getObjectName: ObjectName = ???
}

private[lease] trait MemoryPoolInfo {
  def used(): StorageUnit
  def committed(): StorageUnit
}

private[lease] class MemoryUsageInfo(usage: MemoryUsage) extends MemoryPoolInfo {
  def used(): StorageUnit = usage.getUsed().bytes
  def committed(): StorageUnit = usage.getCommitted().bytes
}

private[lease] case class FakeMemoryUsage(used: StorageUnit, committed: StorageUnit)
    extends MemoryPoolInfo

private[lease] class JvmInfo(val pool: MemoryPool, val collector: GarbageCollectorMXBean) {
  def committed(): StorageUnit = pool.snapshot().committed()
  def used(): StorageUnit = pool.snapshot().used()
  def generation(): Long = collector.getCollectionCount()

  def remaining(): StorageUnit = {
    val snap = pool.snapshot()
    snap.committed() - snap.used()
  }

  def record(lr: LogsReceiver, state: String): Unit = {
    val snap = pool.snapshot()

    lr.record("com_%s".format(state), snap.committed().toString)
    lr.record("use_%s".format(state), snap.used().toString)
    lr.record("byte_%s".format(state), (snap.committed() - snap.used()).toString)
    lr.record("gen_%s".format(state), generation().toString)
  }

  override def toString(): String =
    "JvmInfo(committed" + committed() + ", generation=" + generation() + ", used=" + used() + ", remaining=" + remaining() + ")"
}
