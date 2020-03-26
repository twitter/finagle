package com.twitter.finagle.mux.lease.exp

import java.lang.management.GarbageCollectorMXBean

class GarbageCollectorAddable(self: GarbageCollectorMXBean) {
  def +(other: GarbageCollectorMXBean): GarbageCollectorMXBean = new GarbageCollectorMXBean {
    def getCollectionCount() =
      self.getCollectionCount() + other.getCollectionCount()
    def getCollectionTime() =
      self.getCollectionTime() + other.getCollectionTime()
    def getMemoryPoolNames() =
      Array.concat(self.getMemoryPoolNames(), other.getMemoryPoolNames())
    def getName() = self.getName() + "+" + other.getName()
    def isValid() = self.isValid || other.isValid
    def getObjectName = throw new UnsupportedOperationException
  }
}
